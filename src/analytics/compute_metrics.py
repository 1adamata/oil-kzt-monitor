"""Compute rolling oil/FX relationship metrics from curated aligned bars.

Why it exists:
    The dashboard and downstream analytics need compact, queryable indicators of
    how oil returns and USD/KZT returns move together over recent history.

Key concepts:
    - Loads one-day aligned return bars from TimescaleDB.
    - Computes rolling Pearson correlation to measure co-movement.
    - Computes rolling beta as covariance divided by oil-return variance.
    - Computes spread z-score to show how unusual the return gap is.
    - Writes metric rows in a long format: one row per timestamp/window/metric.

Dependencies worth knowing:
    - pandas provides rolling-window statistics over ordered time-series rows.
    - numpy is available for numeric work alongside pandas.
    - psycopg2 reads from and writes to PostgreSQL/TimescaleDB.
    - python-dotenv loads local database credentials from `.env`.
"""
import os
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# ============================================================
# Database configuration and metric windows
# ============================================================

DB_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market_data",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
}

WINDOWS = {"30d": 30, "60d": 60}


# ============================================================
# Data loading
# ============================================================

def load_aligned(symbol: str) -> pd.DataFrame:
    """Load one-day aligned return bars for one oil benchmark.

    Metrics depend on chronological rolling windows, so the SQL query sorts by
    `bucket_time` before pandas receives the data. The query reads only return
    columns because price levels are not needed for the relationship metrics here.

    Args:
        symbol (str): Oil benchmark to load, for example `brent` or `wti`.

    Returns:
        pandas.DataFrame: DataFrame with columns `bucket_time`, `oil_logret`, and
            `kzt_logret`. Shape is one row per aligned daily bar for the symbol.

    Raises:
        psycopg2.Error: If the database connection or query fails.
        KeyError: If the expected `POSTGRES_PASSWORD` environment variable is absent
            at module import time.

    Example:
        >>> df = load_aligned("brent")
        >>> list(df.columns)
        ['bucket_time', 'oil_logret', 'kzt_logret']

    Notes:
        - The table is filtered to `horizon = '1d'` because the rolling window sizes
          below are expressed in daily rows.
        - `pd.to_datetime` normalizes driver-returned timestamp objects into pandas'
          datetime dtype for reliable indexing and display.
    """
    conn = psycopg2.connect(**DB_CONN)
    df = pd.read_sql("""
        SELECT bucket_time, oil_logret, kzt_logret
        FROM md.curated_aligned_bars
        WHERE oil_symbol = %s AND horizon = '1d'
        ORDER BY bucket_time
    """, conn, params=(symbol,))
    conn.close()
    df["bucket_time"] = pd.to_datetime(df["bucket_time"])
    return df


# ============================================================
# Rolling metric calculation
# ============================================================

def compute(df: pd.DataFrame, symbol: str, window_name: str, window: int) -> list[tuple]:
    """Compute rolling relationship metrics for one symbol and window length.

    The function returns database-ready tuples rather than another DataFrame because
    the next step is a batch insert into `md.relationship_metrics`. Each metric is
    stored in long format so dashboards can filter by metric name without needing a
    separate column for every statistic.

    Args:
        df (pandas.DataFrame): Chronologically sorted aligned bars with columns
            `bucket_time`, `oil_logret`, and `kzt_logret`; shape is `(n_days, 3+)`.
        symbol (str): Oil benchmark label copied into output rows.
        window_name (str): Human-readable window label, such as `30d` or `60d`.
        window (int): Rolling window size measured in rows/days.

    Returns:
        list[tuple]: Rows shaped as `(bucket_time, horizon, oil_symbol, metric,
            value, meta)`, ready for `execute_values`.

    Example:
        >>> dates = pd.date_range("2024-01-01", periods=20, freq="D")
        >>> df = pd.DataFrame({
        ...     "bucket_time": dates,
        ...     "oil_logret": np.linspace(-0.01, 0.02, 20),
        ...     "kzt_logret": np.linspace(0.00, 0.01, 20),
        ... })
        >>> rows = compute(df, "brent", "10d", 10)
        >>> rows[0][2:4]
        ('brent', 'pearson_corr')

    Notes:
        - `min_periods=window // 2` emits early metrics once half a window exists,
          trading statistical stability for faster availability on short histories.
        - Beta is `cov(kzt_logret, oil_logret) / var(oil_logret)`, so it answers:
          "how much does KZT return tend to move per unit of oil return?"
        - Spread z-score standardizes `oil_logret - kzt_logret` by its rolling mean
          and standard deviation; values near zero are typical for that window.
    """
    # Rolling windows operate over row order, so `load_aligned` must keep the time
    # series sorted by bucket_time before this function runs.
    roll = df[["oil_logret", "kzt_logret"]].rolling(window, min_periods=window // 2)

    # Pearson correlation is scale-free: it measures co-movement direction/strength,
    # not whether oil and FX returns have the same magnitude.
    corr = roll.corr().unstack()["oil_logret"]["kzt_logret"]
    # Rolling beta uses oil returns as the explanatory series and KZT returns as the
    # response series, matching the covariance/variance definition from regression.
    beta = (
        df["kzt_logret"].rolling(window, min_periods=window // 2).cov(df["oil_logret"])
        / df["oil_logret"].rolling(window, min_periods=window // 2).var()
    )
    # The spread is a simple relative-return gap; z-scoring makes it comparable
    # across windows with different volatility regimes.
    spread = df["oil_logret"] - df["kzt_logret"]
    spread_mean = spread.rolling(window, min_periods=window // 2).mean()
    spread_std = spread.rolling(window, min_periods=window // 2).std()
    zscore = (spread - spread_mean) / spread_std

    rows = []
    for i, ts in enumerate(df["bucket_time"]):
        for metric, series in [("pearson_corr", corr), ("beta", beta), ("spread_zscore", zscore)]:
            val = series.iloc[i]
            if pd.isna(val):
                continue
            # `meta` is reserved for future structured details, so it is inserted as
            # NULL while keeping the table contract stable.
            rows.append((ts.to_pydatetime(), window_name, symbol, metric, float(val), None))
    return rows


# ============================================================
# Database write
# ============================================================

def write_metrics(rows: list[tuple]) -> None:
    """Insert computed relationship metrics into TimescaleDB.

    The function expects pre-shaped rows from `compute` and writes them in one
    `execute_values` call to reduce database round trips.

    Args:
        rows (list[tuple]): Metric rows shaped as `(bucket_time, horizon, oil_symbol,
            metric, value, meta)`.

    Returns:
        None: Inserts rows into `md.relationship_metrics` and prints the count.

    Raises:
        psycopg2.Error: If the database connection or insert fails.

    Example:
        >>> # write_metrics(compute(df, "brent", "30d", 30))

    Notes:
        - Unlike `align_bars.write_to_db`, this insert has no `ON CONFLICT` clause.
          Rerunning this script may duplicate rows unless the database table handles
          conflicts elsewhere.
        - Empty `rows` is still passed through; psycopg2 may raise if there is
          nothing to interpolate, so callers should ensure there is enough history.
    """
    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO md.relationship_metrics
                    (bucket_time, horizon, oil_symbol, metric, value, meta)
                VALUES %s
            """, rows)
        conn.commit()
        print(f"  Wrote {len(rows)} metric rows")
    finally:
        conn.close()


def main():
    """Compute and persist all configured metrics for Brent and WTI.

    The script processes each oil symbol independently, computes every configured
    rolling window, and writes all metric rows for that symbol in one batch.

    Returns:
        None: Writes metric rows and prints progress messages.

    Example:
        >>> # From the repository root with curated bars already loaded:
        >>> # python src/analytics/compute_metrics.py

    Notes:
        - This batch job should run after `align_bars.py`, because it depends on
          `md.curated_aligned_bars`.
        - Add windows by extending the `WINDOWS` dictionary; no query changes are
          needed because all windows use the same aligned input.
    """
    for symbol in ["brent", "wti"]:
        print(f"Computing metrics for {symbol}...")
        df = load_aligned(symbol)
        all_rows = []
        for window_name, window in WINDOWS.items():
            rows = compute(df, symbol, window_name, window)
            all_rows.extend(rows)
        write_metrics(all_rows)
    print("Done.")


if __name__ == "__main__":
    main()
