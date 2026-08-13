"""
DOO4730 — single-pass two-tier detection check (for GitHub Actions cron).
Runs once: pulls the last few minutes from InfluxDB, applies the two-tier
detector, and appends any NEW confirmed failures to detections.csv.

Tier 1  surge          current jumps over the self-set alarm level (normal cutting)
Tier 2  CONFIRMED       a surge that coincides with a machine stoppage
        FAILURE         (INTERRUPTED/DISCONNECTED within 30s) or a sensor dropout
Only Tier 2 is recorded to detections.csv.
"""
import warnings; warnings.filterwarnings("ignore")
import os, csv
from datetime import datetime
import pandas as pd
from influx_utils import get_client

CUR = ["spindle_current_leg1", "spindle_current_leg2", "spindle_current_leg3"]
ALARM = 89.4
CONFIRM_S = 30
HEARTBEAT_GAP_S = 3.0
ABNORMAL = ["INTERRUPTED", "DISCONNECTED"]
LOOKBACK_MIN = 12
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


def main():
    now = datetime.utcnow().isoformat()
    c = get_client()
    sen = c.query(
        f"SELECT time,{','.join(CUR)} FROM sensor_telemetry WHERE device_id='DOO4730' "
        f"AND time > now() - INTERVAL '{LOOKBACK_MIN} minutes' ORDER BY time ASC",
        language="sql").to_pandas()
    sta = c.query(
        f"SELECT time,run_status FROM telemetry_raw WHERE device_id='DOO4730' "
        f"AND time > now() - INTERVAL '{LOOKBACK_MIN} minutes' ORDER BY time ASC",
        language="sql").to_pandas()

    if not len(sen):
        print(f"{now}Z  no sensor rows in last {LOOKBACK_MIN} min (feed idle) — nothing to do")
        return

    sen["time"] = pd.to_datetime(sen["time"]).dt.tz_localize(None)
    sen = sen.sort_values("time").reset_index(drop=True)
    sen["max_current"] = sen[CUR].max(axis=1)
    sen["surge"] = sen["max_current"] - sen["max_current"].shift(2)
    sen["next_gap"] = (sen["time"].shift(-1) - sen["time"]).dt.total_seconds()

    abn_times = []
    if sta is not None and len(sta):
        sta["time"] = pd.to_datetime(sta["time"]).dt.tz_localize(None)
        abn_times = list(sta[sta["run_status"].isin(ABNORMAL)]["time"])

    keys = existing_keys()
    new_rows = []
    n_surge = 0

    fl = sen[sen["surge"] > ALARM].copy()
    if len(fl):
        g = fl["time"].diff().dt.total_seconds()
        fl["ep"] = (g > 30).cumsum()
        for _, grp in fl.groupby("ep"):
            n_surge += 1
            t0 = grp["time"].iloc[0]
            peak = float(grp["max_current"].max())
            confirmed = any((et >= t0 - pd.Timedelta(seconds=10)) and
                            (et <= t0 + pd.Timedelta(seconds=CONFIRM_S)) for et in abn_times)
            if confirmed:
                key = t0.strftime("%Y%m%d%H%M%S")
                if key not in keys:
                    new_rows.append([key, now, str(t0), round(peak, 1), "surge+stoppage"])
                    keys.add(key)

    # sensor dropout (heartbeat) within the window
    big_gap = sen["next_gap"].max()
    if pd.notna(big_gap) and big_gap > HEARTBEAT_GAP_S:
        gt = sen.loc[sen["next_gap"].idxmax(), "time"]
        key = "HB" + gt.strftime("%Y%m%d%H%M%S")
        if key not in keys:
            new_rows.append([key, now, str(gt), 0, f"sensor_dropout_{big_gap:.0f}s"])
            keys.add(key)

    if new_rows:
        append_rows(new_rows)
        print(f"{now}Z  🚨 {len(new_rows)} NEW confirmed detection(s) recorded:")
        for r in new_rows:
            print("   ", r)
    else:
        print(f"{now}Z  ok · surge episodes={n_surge} · confirmed failures=0 · "
              f"latest sensor={str(sen['time'].max())[:19]}")


if __name__ == "__main__":
    main()
