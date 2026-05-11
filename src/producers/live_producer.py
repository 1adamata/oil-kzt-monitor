"""
Live daily polling adapter.
Fetches Brent, WTI, and USD/KZT from real APIs on a daily schedule
and publishes MarketEnvelope messages to Kafka.
"""
import os
import time
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Producer

from src.common.schemas import MarketEnvelope

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BROKER = "localhost:19092"
EIA_KEY = os.environ["EIA_API_KEY"]
EIA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
NBK_URL = "https://nationalbank.kz/rss/get_rates.cfm"

INSTRUMENTS = {
    "brent": "EPCBRENT",
    "wti":   "EPCWTI",
}

MAX_RETRIES = 3
RETRY_BASE_SECONDS = 60  # 1 minute, doubles each retry


def with_retry(fn, *args, **kwargs):
    """Call fn with exponential backoff on failure."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {fn.__name__}")


def fetch_oil_price(symbol: str, product_code: str, target_date: date) -> float:
    params = {
        "api_key": EIA_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[product][]": product_code,
        "start": target_date.isoformat(),
        "end": target_date.isoformat(),
        "length": 5,
    }
    response = requests.get(EIA_URL, params=params, timeout=15)
    response.raise_for_status()
    rows = response.json()["response"]["data"]
    if not rows:
        raise ValueError(f"No EIA data for {symbol} on {target_date}")
    return float(rows[0]["value"])


def fetch_usdkzt(target_date: date) -> float:
    params = {"fdate": target_date.strftime("%d.%m.%Y")}
    response = requests.get(NBK_URL, params=params, timeout=15)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    for item in root.iter("item"):
        if item.findtext("title", "").strip() == "USD":
            return float(item.findtext("description", "0").strip())
    raise ValueError(f"USD/KZT not found in NBK response for {target_date}")


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BROKER})


def publish(producer: Producer, topic: str, envelope: MarketEnvelope) -> None:
    producer.produce(topic, envelope.model_dump_json().encode())
    producer.flush()


def poll_once(producer: Producer, target_date: date) -> None:
    now = datetime.now(timezone.utc)

    for name, code in INSTRUMENTS.items():
        try:
            price = with_retry(fetch_oil_price, name, code, target_date)
            envelope = MarketEnvelope(
                source="eia",
                instrument_id=name,
                event_time_utc=datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc),
                ingest_time_utc=now,
                payload={"price": price},
            )
            publish(producer, "raw.oil_prices", envelope)
            log.info(f"Published {name} {target_date} price={price}")
        except Exception as e:
            log.error(f"Failed to fetch/publish {name}: {e}")

    try:
        rate = with_retry(fetch_usdkzt, target_date)
        envelope = MarketEnvelope(
            source="nbk",
            instrument_id="usdkzt",
            event_time_utc=datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc),
            ingest_time_utc=now,
            payload={"rate": rate},
        )
        publish(producer, "raw.fx_rates", envelope)
        log.info(f"Published usdkzt {target_date} rate={rate}")
    except Exception as e:
        log.error(f"Failed to fetch/publish usdkzt: {e}")

def get_latest_eia_date() -> date:
    """Find the most recent date EIA has data for."""
    params = {
        "api_key": EIA_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[product][]": "EPCBRENT",
        "length": 1,
    }
    response = requests.get(EIA_URL, params=params, timeout=15)
    response.raise_for_status()
    rows = response.json()["response"]["data"]
    if not rows:
        raise ValueError("No EIA data returned")
    return date.fromisoformat(rows[0]["period"])

def main():
    producer = make_producer()
    log.info("Live producer started. Polling daily.")

    while True:
        target_date = with_retry(get_latest_eia_date)
        log.info(f"Latest EIA date: {target_date}. Polling...")
        poll_once(producer, target_date)

        now = datetime.now(timezone.utc)
        next_run = datetime.combine(
            date.today() + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc
        ) + timedelta(hours=1)
        sleep_seconds = (next_run - now).total_seconds()
        log.info(f"Next poll in {sleep_seconds/3600:.1f} hours.")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()