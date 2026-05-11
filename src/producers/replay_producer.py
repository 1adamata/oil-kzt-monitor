"""Replay historical market-data corpus files into Kafka as MarketEnvelope messages.

Why it exists:
    The streaming pipeline needs deterministic sample data for local development,
    demos, and backfills without depending on live external APIs.

Key concepts:
    - Parquet stores typed historical data compactly for repeatable replays.
    - Kafka topics receive the same envelope shape that live producers would emit.
    - Replay pacing compresses calendar time into short sleeps between messages.
    - Oil and FX rows share the envelope contract but use different payload keys.

Dependencies worth knowing:
    - pandas reads the local Parquet corpus into tabular DataFrames.
    - confluent-kafka is the native Kafka client used to publish encoded messages.
    - Pydantic serialization from `MarketEnvelope` keeps JSON output consistent.
"""
import os
import sys
from pathlib import Path

# ============================================================
# Import path setup
# ============================================================

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

# ============================================================
# Replay configuration
# ============================================================

BROKER = "localhost:19092"
CORPUS_DIR = Path("data/corpus")

# 1 calendar day compressed into this many seconds
SECONDS_PER_DAY = 0.1


# ============================================================
# Kafka publishing helpers
# ============================================================

def make_producer() -> Producer:
    """Create a Kafka producer connected to the local Redpanda/Kafka broker.

    The producer is intentionally configured with only the bootstrap server so the
    replay script stays close to the default Kafka behavior used in local Docker
    environments.

    Returns:
        Producer: A confluent-kafka producer ready to publish to configured topics.

    Example:
        >>> producer = make_producer()
        >>> isinstance(producer, Producer)
        True

    Notes:
        - `BROKER` points at the host-exposed listener, not the Docker-internal
          listener used by Spark containers.
        - Delivery is asynchronous until `flush()` is called by the replay routines.
    """
    return Producer({"bootstrap.servers": BROKER})


def publish(producer: Producer, topic: str, envelope: MarketEnvelope) -> None:
    """Serialize one market envelope and queue it for Kafka publication.

    `produce` is asynchronous: it places the message into the producer's internal
    queue and returns before the broker acknowledges it. The caller flushes after a
    replay batch so all queued messages are delivered before the script exits.

    Args:
        producer (Producer): Kafka producer created by `make_producer`.
        topic (str): Kafka topic name, such as `raw.oil_prices`.
        envelope (MarketEnvelope): Validated message object to publish. Its JSON
            representation is encoded as UTF-8 bytes for Kafka.

    Returns:
        None: Publication is queued as a side effect.

    Example:
        >>> from datetime import datetime, timezone
        >>> env = MarketEnvelope(
        ...     source="example",
        ...     instrument_id="brent",
        ...     event_time_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ...     ingest_time_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ...     payload={"price": 82.5},
        ... )
        >>> # publish(producer, "raw.oil_prices", env)

    Notes:
        - The ingest Spark job expects the Kafka value to be a JSON string matching
          the MarketEnvelope schema.
        - No key is set, so Kafka partitions messages according to producer defaults.
    """
    producer.produce(topic, envelope.model_dump_json().encode())


# ============================================================
# Corpus replay routines
# ============================================================

def replay_oil(producer: Producer, name: str, topic: str) -> None:
    """Replay one oil-price Parquet file into a Kafka topic.

    Each row in `{name}_daily.parquet` becomes one MarketEnvelope with the oil
    instrument id and a `payload.price` value. The source is fixed as `eia` because
    the corpus represents Energy Information Administration-style oil price data.

    Args:
        producer (Producer): Kafka producer used to queue messages.
        name (str): Oil instrument name matching a corpus file stem, for example
            `brent` or `wti`.
        topic (str): Kafka topic to receive the replayed oil messages.

    Returns:
        None: Messages are published to Kafka and flushed before returning.

    Raises:
        FileNotFoundError: If `data/corpus/{name}_daily.parquet` is missing.
        Exception: Propagates pandas or Kafka client errors raised during replay.

    Example:
        >>> producer = make_producer()
        >>> # replay_oil(producer, "brent", "raw.oil_prices")

    Notes:
        - The loop uses `iterrows()` for readability and small demo data; vectorized
          pandas operations would be faster for large historical backfills.
        - One calendar day is compressed to `SECONDS_PER_DAY` seconds, so consumers
          see time-ordered events without waiting in real time.
    """
    df = pd.read_parquet(CORPUS_DIR / f"{name}_daily.parquet")
    print(f"Replaying {len(df)} rows of {name} → {topic}")
    # A single ingest timestamp makes one replay run easy to identify downstream.
    now = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        # Corpus dates are daily observations; attaching UTC makes the envelope
        # unambiguous before Spark converts strings back into timestamps.
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
        # Pacing prevents a local stack from receiving all historical records at once.
        time.sleep(SECONDS_PER_DAY)

    producer.flush()


def replay_fx(producer: Producer) -> None:
    """Replay the USD/KZT FX Parquet file into the raw FX Kafka topic.

    Each row in `usdkzt_daily.parquet` becomes one MarketEnvelope with
    `instrument_id='usdkzt'` and a `payload.rate` value. The payload key differs
    from oil because the downstream TimescaleDB table stores FX rates, not prices.

    Args:
        producer (Producer): Kafka producer used to queue messages.

    Returns:
        None: Messages are published to `raw.fx_rates` and flushed before returning.

    Raises:
        FileNotFoundError: If `data/corpus/usdkzt_daily.parquet` is missing.
        Exception: Propagates pandas or Kafka client errors raised during replay.

    Example:
        >>> producer = make_producer()
        >>> # replay_fx(producer)

    Notes:
        - `source='nbk'` marks the replayed series as National Bank of Kazakhstan
          data, matching the semantic source used by downstream consumers.
        - This function hard-codes the FX topic because the script currently replays
          only one FX pair.
    """
    df = pd.read_parquet(CORPUS_DIR / "usdkzt_daily.parquet")
    print(f"Replaying {len(df)} rows of usdkzt → raw.fx_rates")
    # Keep the replay batch identifiable while preserving each row's historical date.
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
    """Run a full local replay for Brent, WTI, and USD/KZT series.

    The ordering is deterministic: both oil series are sent first to the shared oil
    topic, followed by the FX series. That makes local debugging easier because the
    printed output and Kafka topic contents are predictable across runs.

    Returns:
        None: Publishes all configured corpus files and prints completion status.

    Example:
        >>> # From the repository root with Redpanda running:
        >>> # python src/producers/replay_producer.py

    Notes:
        - A single producer is reused across all series to avoid reconnecting between
          small replay batches.
        - Each replay routine flushes after its own series, so failures are easier to
          localize to a specific corpus file.
    """
    producer = make_producer()
    replay_oil(producer, "brent", "raw.oil_prices")
    replay_oil(producer, "wti", "raw.oil_prices")
    replay_fx(producer)
    print("Replay complete.")


if __name__ == "__main__":
    main()
