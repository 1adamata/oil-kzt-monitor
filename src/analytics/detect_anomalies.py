"""
Runs CUSUM and changepoint detection on relationship metrics
and writes alerts to md.alerts.
"""
import os
import pandas as pd
import numpy as np
import ruptures as rpt
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import timezone

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DB_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market_data",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
}

# CUSUM parameters
CUSUM_THRESHOLD = 1.5   # z-score deviation that starts accumulating
CUSUM_TRIGGER   = 4.0   # accumulated sum that fires an alert


def load_metric(symbol: str, metric: str, horizon: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT bucket_time, value
                FROM md.relationship_metrics
                WHERE oil_symbol = %s AND metric = %s AND horizon = %s
                ORDER BY bucket_time
            """, (symbol, metric, horizon))
            df = pd.DataFrame(cur.fetchall(), columns=["bucket_time", "value"])
    finally:
        conn.close()
    df["bucket_time"] = pd.to_datetime(df["bucket_time"])
    return df


def run_cusum(series: pd.Series) -> pd.Series:
    """Returns the CUSUM accumulator series."""
    s = np.zeros(len(series))
    for i in range(1, len(series)):
        s[i] = max(0, s[i-1] + (abs(series.iloc[i]) - CUSUM_THRESHOLD))
    return pd.Series(s, index=series.index)


def run_bocpd(series: pd.Series) -> list[int]:
    """Returns indices where changepoints were detected."""
    values = series.dropna().values.reshape(-1, 1)
    algo = rpt.Pelt(model="rbf").fit(values)
    breakpoints = algo.predict(pen=3)
    return breakpoints[:-1]  # last element is always len(series)


def find_cusum_alerts(df: pd.DataFrame, cusum: pd.Series, symbol: str, horizon: str) -> list[tuple]:
    alerts = []
    triggered = False
    for i in range(len(df)):
        if cusum.iloc[i] >= CUSUM_TRIGGER and not triggered:
            triggered = True
            ts = df["bucket_time"].iloc[i].to_pydatetime().replace(tzinfo=timezone.utc)
            alerts.append((
                ts, None, "cusum_zscore_breach", "warning",
                symbol, horizon,
                f"CUSUM accumulator reached {cusum.iloc[i]:.2f} on spread_zscore",
                None
            ))
        elif cusum.iloc[i] < CUSUM_THRESHOLD:
            triggered = False
    return alerts


def find_bocpd_alerts(df: pd.DataFrame, breakpoints: list[int], symbol: str, horizon: str) -> list[tuple]:
    alerts = []
    for bp in breakpoints:
        if bp >= len(df):
            continue
        ts = df["bucket_time"].iloc[bp].to_pydatetime().replace(tzinfo=timezone.utc)
        alerts.append((
            ts, None, "regime_change", "warning",
            symbol, horizon,
            f"Statistical regime change detected at index {bp}",
            None
        ))
    return alerts


def write_alerts(rows: list[tuple]) -> None:
    if not rows:
        print("  No alerts to write.")
        return
    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO md.alerts
                    (fired_at, resolved_at, kind, severity, oil_symbol,
                    horizon, description, payload)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, rows)
        conn.commit()
        print(f"  Wrote {len(rows)} alerts")
    finally:
        conn.close()


def main():
    all_alerts = []

    for symbol in ["brent", "wti"]:
        for horizon in ["30d", "60d"]:
            print(f"Analyzing {symbol} {horizon}...")
            df = load_metric(symbol, "spread_zscore", horizon)

            # CUSUM
            cusum = run_cusum(df["value"])
            cusum_alerts = find_cusum_alerts(df, cusum, symbol, horizon)
            print(f"  CUSUM: {len(cusum_alerts)} alerts")

            # BOCPD via ruptures
            bocpd_alerts = find_bocpd_alerts(
                df, run_bocpd(df["value"]), symbol, horizon
            )
            print(f"  Changepoints: {len(bocpd_alerts)} alerts")

            all_alerts.extend(cusum_alerts)
            all_alerts.extend(bocpd_alerts)

    write_alerts(all_alerts)
    print("Done.")


if __name__ == "__main__":
    main()
