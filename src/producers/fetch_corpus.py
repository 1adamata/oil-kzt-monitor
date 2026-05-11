"""Fetch historical Brent and WTI prices and persist them as local Parquet files.

This script builds a small, reproducible corpus of daily oil prices from the
EIA v2 API so downstream analytics and experiments can work against a local
snapshot instead of calling the API repeatedly.

Key concepts:
- Uses the EIA petroleum spot price endpoint as the source of truth.
- Normalizes each response into a simple date/price table.
- Writes one Parquet file per instrument for efficient downstream reads.
- Pulls the API key from the environment to avoid hard-coding credentials.

Dependencies worth knowing:
- requests handles the HTTP call and response validation.
- pandas is used for tabular cleanup and Parquet output.
- pyarrow or fastparquet must be installed for to_parquet to work.
"""
import os
from datetime import date, timedelta
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

API_KEY = os.getenv("EIA_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "EIA_API_KEY is not set. Add it to .env or export it before running "
        "src/producers/fetch_corpus.py."
    )
CORPUS_DIR = Path("data/corpus")
BASE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

INSTRUMENTS = {
    "brent": "EPCBRENT",
    "wti":   "EPCWTI",
}


def fetch_oil(product_code: str, start: date, end: date) -> pd.DataFrame:
    """Fetch a daily oil price series for one EIA product code.

    The EIA API returns a nested JSON payload with daily rows. This helper
    extracts the date and value columns, converts them into a typed DataFrame,
    and sorts the result so callers always receive chronological data.

    Args:
        product_code: EIA product facet code such as EPCBRENT or EPCWTI.
        start: Inclusive start date for the query window.
        end: Inclusive end date for the query window.

    Returns:
        A DataFrame with columns:
        - date: pandas datetime64 values
        - price: floating-point price values

    Raises:
        KeyError: If the EIA payload does not contain the expected fields.
        requests.HTTPError: If the API response is not successful.

    Example:
        >>> from datetime import date
        >>> df = fetch_oil("EPCBRENT", date(2024, 1, 1), date(2024, 1, 7))
        >>> list(df.columns)
        ['date', 'price']

    Notes:
        - The endpoint is queried with a generous length limit so short ranges
          do not need pagination handling.
        - Sorting after the conversion makes the output stable even if the API
          returns rows out of order.
    """
    params = {
        "api_key": API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[product][]": product_code,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "length": 5000,
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    rows = response.json()["response"]["data"]
    df = pd.DataFrame(rows)[["period", "value"]]
    df.columns = ["date", "price"]
    # Normalize the API strings into typed columns so the saved corpus has
    # predictable dtypes for plotting, filtering, and time-series joins.
    df["date"] = pd.to_datetime(df["date"])
    df["price"] = df["price"].astype(float)
    return df.sort_values("date").reset_index(drop=True)

import time
import xml.etree.ElementTree as ET
from datetime import timedelta

def fetch_usdkzt(start: date, end: date) -> pd.DataFrame:
    """Fetch daily USD/KZT rates from NBK RSS endpoint, one request per day."""
    rows = []
    current = start
    while current <= end:
        url = f"https://nationalbank.kz/rss/get_rates.cfm?fdate={current.strftime('%d.%m.%Y')}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            for item in root.iter("item"):
                title = item.findtext("title", "").strip()
                if title == "USD":
                    rate = float(item.findtext("description", "0").strip())
                    rows.append({"date": pd.Timestamp(current), "rate": rate})
                    break
        except Exception as e:
            print(f"  Warning: failed for {current} — {e}")
        time.sleep(0.3)
        current += timedelta(days=1)

    df = pd.DataFrame(rows)
    return df.sort_values("date").reset_index(drop=True)

def main():
    """Fetch the configured instruments and write them to local Parquet files.

    Returns:
        None

    Notes:
        - The corpus directory is created on demand so the script is safe to run
          in a fresh checkout.
        - The default window is the last 365 days, which keeps the dataset small
          enough for quick iteration while still being useful for analysis.
    """
    # Create the output directory lazily so a fresh checkout can run the script
    # without any manual setup.
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    # Use a rolling one-year window to keep the generated corpus compact while
    # still preserving enough history for charting and basic analysis.
    end = date.today()
    start = end - timedelta(days=365)

    for name, code in INSTRUMENTS.items():
        # Keep the loop explicit so each instrument can be fetched and written
        # independently without failing the entire corpus generation step.
        print(f"Fetching {name} ({code}) from {start} to {end}...")
        df = fetch_oil(code, start, end)
        out = CORPUS_DIR / f"{name}_daily.parquet"
        df.to_parquet(out, index=False)
        print(f"  Saved {len(df)} rows → {out}")

    print(f"Fetching USD/KZT from {start} to {end}...")
    df_fx = fetch_usdkzt(start, end)
    out = CORPUS_DIR / "usdkzt_daily.parquet"
    df_fx.to_parquet(out, index=False)
    print(f"  Saved {len(df_fx)} rows → {out}")

if __name__ == "__main__":
    main()
