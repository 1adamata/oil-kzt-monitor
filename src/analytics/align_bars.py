"""Align daily oil and USD/KZT series into curated return bars.

Why it exists:
    Downstream analytics need oil prices, FX rates, and their comparable daily
    returns on the same time buckets instead of in separate raw source tables.

Key concepts:
    - Uses each oil series as the trading-day spine for alignment.
    - Forward-fills FX rates so missing FX observations can still pair with oil days.
    - Computes one-day log returns with `log(current / previous)`.
    - Inserts curated bars idempotently with `ON CONFLICT DO NOTHING`.

Dependencies worth knowing:
    - pandas reads Parquet corpus files and performs time-series alignment.
    - numpy provides vectorized logarithms for return calculations.
    - psycopg2 writes batches efficiently into PostgreSQL/TimescaleDB.
    - python-dotenv loads local database credentials from `.env`.
"""
import os
import numpy as np
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# ============================================================
# Local corpus and database configuration
# ============================================================

CORPUS_DIR = Path("data/corpus")
DB_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market_data",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
}


# ============================================================
# Data loading
# ============================================================

def load_corpus() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the historical oil and FX corpus from local Parquet files.

    The replay and analytics scripts share the same corpus directory so this batch
    job can reproduce the same source observations that are used in the streaming
    demo path.

    Returns:
        tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
            Three DataFrames in `(brent, wti, fx)` order.
            - `brent` and `wti`: expected columns `date` and `price`; shape is one
              row per daily oil observation.
            - `fx`: expected columns `date` and `rate`; shape is one row per daily
              USD/KZT observation.

    Raises:
        FileNotFoundError: If any expected Parquet file is missing.
        ImportError: If pandas cannot find a Parquet engine such as pyarrow.

    Example:
        >>> brent, wti, fx = load_corpus()
        >>> {"date", "price"}.issubset(brent.columns)
        True

    Notes:
        - Parquet preserves dates and numeric values better than CSV for repeated
          local analytics runs.
        - The function intentionally does not clean data; alignment owns the
          date normalization and return calculations.
    """
    brent = pd.read_parquet(CORPUS_DIR / "brent_daily.parquet")
    wti = pd.read_parquet(CORPUS_DIR / "wti_daily.parquet")
    fx = pd.read_parquet(CORPUS_DIR / "usdkzt_daily.parquet")
    return brent, wti, fx


# ============================================================
# Time-series alignment and feature calculation
# ============================================================

def align(oil_df: pd.DataFrame, fx_df: pd.DataFrame, oil_symbol: str) -> pd.DataFrame:
    """Align one oil series with USD/KZT and compute one-day log returns.

    Oil dates define the output calendar. This is useful when the analysis question
    is "what happened to FX on oil trading days?" rather than "what happened on
    every calendar day?" FX values are left-joined and forward-filled so holidays or
    missing FX rows do not automatically remove oil observations.

    Args:
        oil_df (pandas.DataFrame): Oil input with columns:
            - `date`: daily observation date.
            - `price`: oil price level for that date.
            Shape is `(n_oil_days, 2+)`.
        fx_df (pandas.DataFrame): FX input with columns:
            - `date`: daily observation date.
            - `rate`: USD/KZT level for that date.
            Shape is `(n_fx_days, 2+)`.
        oil_symbol (str): Instrument label to write into the output, for example
            `brent` or `wti`.

    Returns:
        pandas.DataFrame: Aligned bars with columns including `date`, `oil_price`,
            `usdkzt`, `oil_logret`, `kzt_logret`, `oil_symbol`, `horizon`,
            `oil_quality`, and `fx_quality`. Shape is at most `(n_oil_days - 1, 9)`
            because the first row has no previous value for return calculation.

    Example:
        >>> oil = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"],
        ...                     "price": [80.0, 84.0]})
        >>> fx = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"],
        ...                    "rate": [450.0, 459.0]})
        >>> out = align(oil, fx, "brent")
        >>> round(out.iloc[0]["oil_logret"], 4)
        0.0488

    Notes:
        - Log returns are additive across time, which makes them more convenient
          than simple percentage returns for many time-series models.
        - Forward-filling FX assumes the latest available rate remains the best
          estimate until a newer observation appears.
        - Rows with missing returns are dropped because return needs both current
          and previous price/rate levels.
    """
    # Oil is the spine: the output keeps oil trading days and only adds FX where it
    # can be matched or carried forward from the latest known value.
    df = oil_df.copy().rename(columns={"price": "oil_price"})
    # Normalizing removes intraday time components so joins compare calendar days.
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    fx = fx_df.copy().rename(columns={"rate": "usdkzt"})
    fx["date"] = pd.to_datetime(fx["date"]).dt.normalize()

    # Left join preserves oil rows; forward fill then handles sparse FX calendars.
    df = df.merge(fx, on="date", how="left")
    df["usdkzt"] = df["usdkzt"].ffill()

    # Log return: log(P_t / P_{t-1}). `shift(1)` aligns each row with yesterday's
    # level; the first row becomes NaN because there is no previous observation.
    df["oil_logret"] = np.log(df["oil_price"] / df["oil_price"].shift(1))
    df["kzt_logret"] = np.log(df["usdkzt"] / df["usdkzt"].shift(1))

    df["oil_symbol"] = oil_symbol
    df["horizon"] = "1d"
    df["oil_quality"] = 0
    df["fx_quality"] = 0

    return df.dropna(subset=["oil_logret", "kzt_logret"])


# ============================================================
# Database write
# ============================================================

def write_to_db(df: pd.DataFrame) -> None:
    """Insert aligned bars into the curated TimescaleDB table.

    The function converts DataFrame rows into tuples expected by
    `execute_values`, which sends a single multi-row INSERT instead of one INSERT
    per row. `ON CONFLICT DO NOTHING` makes reruns safe when the target table has a
    uniqueness constraint for the same logical bar.

    Args:
        df (pandas.DataFrame): Output from `align`. Expected columns are `date`,
            `horizon`, `oil_symbol`, `oil_price`, `oil_quality`, `usdkzt`,
            `fx_quality`, `oil_logret`, and `kzt_logret`; shape is one row per
            aligned daily bar.

    Returns:
        None: Inserts rows into `md.curated_aligned_bars` and prints a row count.

    Raises:
        psycopg2.Error: If the database connection or insert fails.
        KeyError: If `df` is missing any required output column.

    Example:
        >>> # write_to_db(align(brent, fx, "brent"))

    Notes:
        - `execute_values` is a good fit for medium-sized batch inserts from pandas.
        - The connection is closed in `finally` so failures do not leave open local
          connections.
        - `iterrows()` is acceptable for this small corpus; larger batches may need
          a faster conversion path such as `itertuples()`.
    """
    # Convert pandas/numpy scalar values into plain Python values psycopg2 can adapt.
    rows = [
        (
            row["date"].to_pydatetime(),
            row["horizon"],
            row["oil_symbol"],
            row["oil_price"],
            int(row["oil_quality"]),
            row["usdkzt"],
            int(row["fx_quality"]),
            row["oil_logret"],
            row["kzt_logret"],
        )
        for _, row in df.iterrows()
    ]

    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn.cursor() as cur:
            # Multi-row INSERT keeps database round trips low and `ON CONFLICT`
            # makes this batch job repeatable during local development.
            execute_values(cur, """
                INSERT INTO md.curated_aligned_bars
                    (bucket_time, horizon, oil_symbol, oil_price, oil_quality,
                     usdkzt, fx_quality, oil_logret, kzt_logret)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, rows)
        conn.commit()
        print(f"  Wrote {len(rows)} rows to md.curated_aligned_bars")
    finally:
        conn.close()


def main():
    """Build and store curated aligned bars for Brent and WTI.

    The same USD/KZT series is aligned separately with each oil benchmark because
    Brent and WTI can have different missing dates or price histories.

    Returns:
        None: Writes curated rows to the database and prints progress messages.

    Example:
        >>> # From the repository root with TimescaleDB running:
        >>> # python src/analytics/align_bars.py

    Notes:
        - Running this after corpus generation is enough for the batch analytics
          path; it does not depend on Kafka or Spark.
        - Reruns are expected to be idempotent because the insert ignores conflicts.
    """
    brent, wti, fx = load_corpus()

    print("Aligning Brent...")
    brent_aligned = align(brent, fx, "brent")
    write_to_db(brent_aligned)

    print("Aligning WTI...")
    wti_aligned = align(wti, fx, "wti")
    write_to_db(wti_aligned)

    print("Done.")


if __name__ == "__main__":
    main()
