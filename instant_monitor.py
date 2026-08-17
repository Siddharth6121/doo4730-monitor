"""
DOO4730 — INSTANT-ALERT monitor (continuous, always-on)
--------------------------------------------------------
A production-style poller: checks InfluxDB every few SECONDS, runs the two-tier
detector, and emails within seconds of a CONFIRMED failure (no 5-min cron).

Run (on an always-on host / free VM, kept awake):
    python3 instant_monitor.py
    # test the email path once:  python3 instant_monitor.py test-email

Env vars required:
    INFLUX_HOST / INFLUX_TOKEN / INFLUX_DATABASE   (already in .env)
    SMTP_USER      e.g. siddharthc6121@gmail.com   (sending account)
    SMTP_PASS      the Gmail app password
    ALERT_TO       comma-separated recipients
    SMTP_HOST      optional, default smtp.gmail.com
    SMTP_PORT      optional, default 465
    POLL_SECONDS   optional, default 8
"""
import warnings; warnings.filterwarnings("ignore")
import os, sys, csv, time, ssl, json, smtplib, urllib.request
from email.message import EmailMessage
from datetime import datetime
import pandas as pd
from influx_utils import get_client

CUR = ["spindle_current_leg1", "spindle_current_leg2", "spindle_current_leg3"]
ALARM = 89.4
CONFIRM_S = 30
HEARTBEAT_GAP_S = 3.0
ABNORMAL = ["INTERRUPTED", "DISCONNECTED"]
LOOKBACK_MIN = 2                         # small recent window each poll (fast, cheap)
POLL_S = int(os.environ.get("POLL_SECONDS", "2"))   # 1-2s for tightest latency
HEARTBEAT_EVERY = 60                      # log a proof-of-life line every N polls
DET = os.path.join(os.path.dirname(os.path.abspath(__file__)), "detections.csv")
HEADER = ["detected_utc", "event_time_utc", "peak_A", "type"]

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
ALERT_TO = [x.strip() for x in os.environ.get("ALERT_TO", "").split(",") if x.strip()]
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")   # Teams/Slack incoming webhook — fastest push
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", "")   # dead-man's switch (e.g. healthchecks.io) — pings while alive
FEED_STALE_MIN = int(os.environ.get("FEED_STALE_MIN", "10"))   # alert if no new sensor data for this many minutes


def log(m): print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}Z  {m}", flush=True)


def ping_health():
    """Ping the external watchdog to say 'I'm alive'. If these pings stop (crash, VM down,
    network down), the watchdog service alerts you — no dependence on this box being up."""
    if not HEALTHCHECK_URL:
        return
    try:
        urllib.request.urlopen(HEALTHCHECK_URL, timeout=5)
    except Exception:
        pass


def send_email(subject, body):
    if not (SMTP_USER and SMTP_PASS and ALERT_TO):
        log("EMAIL SKIPPED — SMTP_USER / SMTP_PASS / ALERT_TO not set in env")
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"DOO4730 Monitor <{SMTP_USER}>"
    msg["To"] = ", ".join(ALERT_TO)
    msg.set_content(body)
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ssl.create_default_context()) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        log(f"EMAIL sent to {', '.join(ALERT_TO)}")
        return True
    except Exception as e:
        log(f"EMAIL ERROR: {str(e)[:140]}")
        return False


def send_webhook(text):
    """Post to a Teams/Slack incoming webhook — near-instant, deterministic (~1s)."""
    if not WEBHOOK_URL:
        return False
    try:
        req = urllib.request.Request(
            WEBHOOK_URL, data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        log("WEBHOOK posted")
        return True
    except Exception as e:
        log(f"WEBHOOK ERROR: {str(e)[:120]}")
        return False


def existing_keys():
    if not os.path.exists(DET):
        return set()
    try:
        df = pd.read_csv(DET)
        return set(df["event_time_utc"].astype(str)) if "event_time_utc" in df.columns else set()
    except Exception:
        return set()


def append_det(row):
    new = not os.path.exists(DET)
    with open(DET, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEADER)
        w.writerow(row)


def poll_once(client, seen):
    """One detection pass over the recent window. Returns list of NEW confirmed failures."""
    sen = client.query(
        f"SELECT time,{','.join(CUR)} FROM sensor_telemetry WHERE device_id='DOO4730' AND tenant_id=2 "
        f"AND time > now() - INTERVAL '{LOOKBACK_MIN} minutes' ORDER BY time ASC", language="sql").to_pandas()
    sta = client.query(
        f"SELECT time, run_status FROM telemetry_raw WHERE device_id='DOO4730' AND tenant_id=2 "
        f"AND time > now() - INTERVAL '{LOOKBACK_MIN} minutes' ORDER BY time ASC", language="sql").to_pandas()
    if not len(sen):
        return [], "no-sensor"
    sen["time"] = pd.to_datetime(sen["time"]).dt.tz_localize(None)
    sen = sen.sort_values("time").reset_index(drop=True)
    sen["mc"] = sen[CUR].max(axis=1)
    sen["surge"] = sen["mc"] - sen["mc"].shift(2)
    sen["gap"] = (sen["time"].shift(-1) - sen["time"]).dt.total_seconds()
    abn_times = []
    if sta is not None and len(sta):
        sta["time"] = pd.to_datetime(sta["time"]).dt.tz_localize(None)
        abn_times = list(sta[sta["run_status"].isin(ABNORMAL)]["time"])

    new = []
    # Tier-2: surge episodes confirmed by a nearby stoppage
    fl = sen[sen["surge"] > ALARM]
    if len(fl):
        g = fl["time"].diff().dt.total_seconds()
        fl = fl.assign(ep=(g > 30).cumsum())
        for _, grp in fl.groupby("ep"):
            t0 = grp["time"].iloc[0]
            if any((et >= t0 - pd.Timedelta(seconds=10)) and (et <= t0 + pd.Timedelta(seconds=CONFIRM_S)) for et in abn_times):
                key = str(t0)[:19]
                if key not in seen:
                    seen.add(key)
                    new.append((key, round(float(grp["mc"].max()), 1), "surge+stoppage"))
    # heartbeat dropout
    bg = sen["gap"].max()
    if pd.notna(bg) and bg > HEARTBEAT_GAP_S:
        gt = sen.loc[sen["gap"].idxmax(), "time"]
        key = "HB " + str(gt)[:19]
        if key not in seen:
            seen.add(key)
            new.append((str(gt)[:19], 0.0, f"sensor_dropout_{bg:.0f}s"))
    latest = str(sen["time"].max())[:19]
    return new, latest


def main():
    log("=== instant_monitor starting ===")
    log(f"poll every {POLL_S}s · {LOOKBACK_MIN}-min window · email {'ON' if (SMTP_USER and ALERT_TO) else 'OFF (set SMTP env)'}")
    client = get_client(); client.query("SHOW TABLES", language="sql")
    seen = existing_keys()
    n = 0
    feed_alerted = False
    ping_every = max(1, int(30 / POLL_S))   # ping the watchdog ~every 30s
    while True:
        n += 1
        try:
            new, info = poll_once(client, seen)
        except Exception as e:
            # note: we do NOT ping the watchdog here -> if InfluxDB stays unreachable, the
            # dead-man's switch fires and alerts you that the monitor is effectively blind.
            log(f"poll error (retrying): {str(e)[:110]}"); time.sleep(POLL_S); continue
        for key, peak, typ in new:
            now = datetime.utcnow().isoformat()
            append_det([now, key, peak, typ])
            log(f"🚨 CONFIRMED FAILURE  event={key} UTC  peak={peak}A  type={typ}")
            send_webhook(f"🚨 DOO4730 tool failure — event {key} UTC, {typ}, peak {peak} A (detected {now} UTC)")
            send_email(
                "🚨 DOO4730 — tool failure detected",
                f"A confirmed tool failure was detected on DOO4730.\n\n"
                f"Event time (UTC): {key}\nType: {typ}\nPeak current: {peak} A\n"
                f"Detected at: {now} UTC\n\nRecorded to detections.csv.")

        # --- feed-down alert: monitor is up, but is data still arriving? ---
        stale = (info == "no-sensor")
        if not stale:
            try:
                age_min = (datetime.utcnow() - datetime.strptime(info, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
                stale = age_min > FEED_STALE_MIN
            except Exception:
                stale = False
        if stale and not feed_alerted:
            feed_alerted = True
            log(f"⚠️  SENSOR FEED STALE — no new data for >{FEED_STALE_MIN} min (latest={info})")
            send_webhook(f"⚠️ DOO4730 monitor: sensor feed STALLED — no new data for >{FEED_STALE_MIN} min (latest {info} UTC)")
            send_email("⚠️ DOO4730 — sensor feed stalled",
                       f"No new sensor data for over {FEED_STALE_MIN} minutes (latest reading {info} UTC). "
                       f"The detector is running but has no live data to watch — please check the feed/ingestion.")
        elif not stale and feed_alerted:
            feed_alerted = False
            log("✓ sensor feed recovered")
            send_webhook("✓ DOO4730 monitor: sensor feed recovered.")

        # --- dead-man's switch: we reached here => app is alive AND InfluxDB reachable ---
        if n % ping_every == 0:
            ping_health()
        if n % HEARTBEAT_EVERY == 0:
            log(f"heartbeat: alive · latest sensor={info} · confirmed-total={len(seen)}")
        time.sleep(POLL_S)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test-email":
        ok = send_email("✅ DOO4730 monitor — test alert",
                        "This is a test from instant_monitor.py. If you got this, instant email alerts work.")
        sys.exit(0 if ok else 1)
    if len(sys.argv) > 1 and sys.argv[1] == "test-webhook":
        ok = send_webhook("✅ DOO4730 monitor — test alert. If you see this, instant webhook alerts work.")
        sys.exit(0 if ok else 1)
    try:
        main()
    except KeyboardInterrupt:
        log("stopped by user")
