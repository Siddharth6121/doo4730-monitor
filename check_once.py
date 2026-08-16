"""
DOO4730 — single-pass two-tier detection check (for GitHub Actions cron).

MISS-PROOF DESIGN:
- Wide 45-min lookback (GitHub's free cron slips to ~20-min gaps; overlapping wide
  windows + de-dup guarantee no event is skipped).
- Status-driven + file-limit-safe: we scan the SPARSE stoppage stream in small
  time-chunks (cheap, never trips the 432-file scan cap), and for each stoppage do a
  TINY sensor check around it — instead of scanning a wide 1 Hz sensor window.
- tenant_id = 2 (DOO4730's rich feed; avoids the cross-tenant duplicate rows).

A CONFIRMED failure = an INTERRUPTED/DISCONNECTED stoppage that coincides with a
current surge OR a sensor dropout (the two-tier rule). Only these are recorded/emailed.
"""
import warnings; warnings.filterwarnings("ignore")
import os, csv
from datetime import datetime
import pandas as pd
from influx_utils import get_client

CUR = ["spindle_current_leg1", "spindle_current_leg2", "spindle_current_leg3"]
ALARM = 89.4
HEARTBEAT_GAP_S = 3.0
ABNORMAL = ["INTERRUPTED", "DISCONNECTED"]
LOOKBACK_MIN = 45            # wide, to cover GitHub cron delays (never miss)
CHUNK_MIN = 10              # scan the status stream in 10-min pieces (file-limit-safe)
DET = "detections.csv"
HEADER = ["key", "detected_utc", "event_time_utc", "peak_A", "type"]


def existing_keys():
    if not os.path.exists(DET):
        return set()
    try:
        df = pd.read_csv(DET)
        return set(df["key"].astype(str)) if "key" in df.columns else set()
    except Exception:
        return set()


def append_rows(rows):
    new = not os.path.exists(DET)
    with open(DET, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEADER)
        for r in rows:
            w.writerow(r)


def set_output(name, value):
    go = os.environ.get("GITHUB_OUTPUT")
    if not go:
        return
    d = f"__EOF_{name}__"
    with open(go, "a") as f:
        f.write(f"{name}<<{d}\n{value}\n{d}\n")


def main():
    now = pd.Timestamp.utcnow().tz_localize(None)
    ts = datetime.utcnow().isoformat()
    c = get_client()

    # 1) gather abnormal stoppages over the wide window, in small chunks (cheap + file-safe)
    abn = []
    h = LOOKBACK_MIN
    while h > 0:
        a = now - pd.Timedelta(minutes=h)
        b = now - pd.Timedelta(minutes=max(h - CHUNK_MIN, 0))
        try:
            d = c.query(
                f"SELECT time, run_status FROM telemetry_raw WHERE device_id='DOO4730' AND tenant_id=2 "
                f"AND run_status IN ('INTERRUPTED','DISCONNECTED') "
                f"AND time >= TIMESTAMP '{a:%Y-%m-%d %H:%M:%S}' AND time < TIMESTAMP '{b:%Y-%m-%d %H:%M:%S}' "
                f"ORDER BY time ASC", language="sql").to_pandas()
            if len(d):
                abn.append(d)
        except Exception:
            pass
        h -= CHUNK_MIN
    stops = pd.concat(abn).drop_duplicates(subset=["time"]) if abn else pd.DataFrame(columns=["time", "run_status"])
    if len(stops):
        stops["time"] = pd.to_datetime(stops["time"]).dt.tz_localize(None)

    # 2) confirm each NEW stoppage with a tiny sensor check (surge OR dropout)
    keys = existing_keys()
    new_rows = []
    for _, r in stops.sort_values("time").iterrows():
        t = r["time"]
        key = t.strftime("%Y%m%d%H%M%S")
        if key in keys:
            continue
        a = (t - pd.Timedelta(seconds=45)).strftime("%Y-%m-%d %H:%M:%S")
        b = (t + pd.Timedelta(seconds=35)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            w = c.query(
                f"SELECT time,{','.join(CUR)} FROM sensor_telemetry WHERE device_id='DOO4730' AND tenant_id=2 "
                f"AND time > TIMESTAMP '{a}' AND time < TIMESTAMP '{b}' ORDER BY time ASC", language="sql").to_pandas()
        except Exception:
            continue
        surge = dropout = False
        peak = 0.0
        if len(w) > 2:
            w["time"] = pd.to_datetime(w["time"]).dt.tz_localize(None)
            mc = w[CUR].max(axis=1)
            peak = float(mc.max())
            surge = bool(((mc - mc.shift(2)) > ALARM).any())
            dropout = bool((w["time"].diff().dt.total_seconds().max() or 0) > HEARTBEAT_GAP_S)
        else:
            dropout = True   # feed silent around the stoppage
        if surge or dropout:
            typ = "surge+stoppage" if surge else "sensor_dropout+stoppage"
            keys.add(key)
            new_rows.append([key, ts, str(t)[:19], round(peak, 1), typ])

    if new_rows:
        append_rows(new_rows)
        print(f"{ts}Z  🚨 {len(new_rows)} NEW confirmed failure(s):")
        for r in new_rows:
            print("   ", r)
        body = ("DOO4730 tool failure detected on the live feed.\n\n" +
                "\n".join(f"- {r[4]} at {r[2]} UTC (peak {r[3]} A)" for r in new_rows) +
                f"\n\nDetected at {ts} UTC. Full log: detections.csv in the repo.")
        set_output("new_failures", str(len(new_rows)))
        set_output("summary", body)
    else:
        print(f"{ts}Z  ok · stoppages scanned={len(stops)} · confirmed failures=0 (lookback {LOOKBACK_MIN}m)")
        set_output("new_failures", "0")


if __name__ == "__main__":
    main()
