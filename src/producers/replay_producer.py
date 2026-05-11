"""
Reads historical corpus Parquet files and publishes them to Kafka
as MarketEnvelope messages, paced by a configurable speed multiplier.
"""
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path so `src` is importable when running
# this file directly (e.g. `python src/producers/replay_producer.py`).
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import time
from datetime import datetime, timezone
import pandas as pd
from confluent_kafka import Producer

from src.common.schemas import MarketEnvelope

BROKER = "localhost:19092"
CORPUS_DIR = Path("data/corpus")

# 1 calendar day compressed into this many seconds
SECONDS_PER_DAY = 0.1


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BROKER})


def publish(producer: Producer, topic: str, envelope: MarketEnvelope) -> None:
    producer.produce(topic, envelope.model_dump_json().encode())


def replay_oil(producer: Producer, name: str, topic: str) -> None:
    df = pd.read_parquet(CORPUS_DIR / f"{name}_daily.parquet")
    print(f"Replaying {len(df)} rows of {name} → {topic}")
    now = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        event_time = row["date"].to_pydatetime().replace(tzinfo=timezone.utc)
        envelope = MarketEnvelope(
            source="eia",
            instrument_id=name,
            event_time_utc=event_time,
            ingest_time_utc=now,
            payload={"price": row["price"]},
        )
        publish(producer, topic, envelope)
        print(f"  {event_time.date()} | {name} | {row['price']:.2f}")
        time.sleep(SECONDS_PER_DAY)

    producer.flush()


def replay_fx(producer: Producer) -> None:
    df = pd.read_parquet(CORPUS_DIR / "usdkzt_daily.parquet")
    print(f"Replaying {len(df)} rows of usdkzt → raw.fx_rates")
    now = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        event_time = row["date"].to_pydatetime().replace(tzinfo=timezone.utc)
        envelope = MarketEnvelope(
            source="nbk",
            instrument_id="usdkzt",
            event_time_utc=event_time,
            ingest_time_utc=now,
            payload={"rate": row["rate"]},
        )
        publish(producer, "raw.fx_rates", envelope)
        print(f"  {event_time.date()} | usdkzt | {row['rate']:.2f}")
        time.sleep(SECONDS_PER_DAY)

    producer.flush()


def main():
    producer = make_producer()
    replay_oil(producer, "brent", "raw.oil_prices")
    replay_oil(producer, "wti", "raw.oil_prices")
    replay_fx(producer)
    print("Replay complete.")


if __name__ == "__main__":
    main()