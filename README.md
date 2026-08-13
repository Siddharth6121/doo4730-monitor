# DOO4730 — always-on failure monitor (GitHub Actions)

Runs the two-tier tool-failure detector on the live InfluxDB feed **every 5 minutes**,
with no server to manage. Any **confirmed failure** is appended to
[`detections.csv`](detections.csv) and committed automatically, so you have a
persistent record to check any time.

## How it works
- A scheduled GitHub Actions workflow (`.github/workflows/monitor.yml`) fires every 5 min.
- `check_once.py` pulls the last ~12 minutes of `sensor_telemetry` (current) and
  `telemetry_raw` (machine status) for `DOO4730`, then applies:
  - **Tier 1 — surge:** current jumps over the self-calibrated alarm level (~89 A).
    Normal cutting does this constantly, so it is *not* recorded on its own.
  - **Tier 2 — confirmed failure:** a surge that coincides with a machine stoppage
    (`INTERRUPTED`/`DISCONNECTED` within 30 s) **or** a sensor dropout.
    Only Tier 2 is written to `detections.csv`.

## Checking results
- Open **`detections.csv`** — each row is a confirmed failure (timestamp, peak current, type).
  Empty = nothing failed.
- The **Actions tab** shows a green run every ~5 min = proof it's alive. Click any run to
  see the per-check summary (surge counts, latest reading).

## Setup (already done if this was pushed for you)
Repo secrets required (Settings → Secrets and variables → Actions):
`INFLUX_HOST`, `INFLUX_TOKEN`, `INFLUX_DATABASE`. Credentials are **never** in the code.

## Notes / limits
- GitHub cron runs at ~5-min granularity and can be delayed under load — this is a
  monitoring log, not a to-the-second alarm.
- Detection is at the *moment* of failure (surge + stoppage), not a prediction ahead of time.
- To stop it: disable the workflow in the Actions tab.
