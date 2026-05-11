"""Ingest raw market data from Kafka into TimescaleDB with Spark Structured Streaming.

Why it exists:
    This job bridges append-only Kafka topics and analytical TimescaleDB hypertables so
    downstream services can query oil and FX observations with SQL.

Key concepts:
    - Spark Structured Streaming treats Kafka input as an unbounded DataFrame.
    - `foreachBatch` reuses normal batch DataFrame writes for each micro-batch.
    - A shared MarketEnvelope JSON schema keeps producers and consumers loosely coupled.
    - Separate checkpoint directories let oil and FX sinks track progress independently.

Dependencies worth knowing:
    - PySpark reads Kafka streams, parses JSON, and performs micro-batch writes.
    - PostgreSQL JDBC driver lets Spark append rows directly to TimescaleDB tables.
    - `.env` can provide local database credentials without hard-coding secrets.
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, MapType, DoubleType
)

# ============================================================
# Connection configuration & message schema
# ============================================================

def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE pairs into `os.environ` if they are not already set.

    Spark's base Python image does not include `python-dotenv`, so this parser keeps
    the ingest job self-contained. It intentionally supports only the `.env` format
    used by this project: comments, blank lines, optional `export`, and unquoted or
    simply quoted values.

    Args:
        path (Path): Candidate `.env` file path.

    Returns:
        None: Updates `os.environ` as a side effect.

    Example:
        >>> # load_env_file(Path(".env"))

    Notes:
        - Existing environment variables win over file values. That lets Docker,
          CI, or `spark-submit --conf spark.executorEnv...` override local files.
        - Missing files are ignored so the same code runs locally and in containers.
    """
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


# Local runs resolve to the repo root; Spark container runs resolve to /opt/spark-apps.
load_env_file(Path(__file__).resolve().parents[2] / ".env")

KAFKA_BROKER = "redpanda-0:9092"
JDBC_URL = "jdbc:postgresql://timescaledb:5432/market_data"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not POSTGRES_PASSWORD:
    raise RuntimeError(
        "POSTGRES_PASSWORD is not set. Add it to .env or export it before running "
        "src/streaming/ingest_job.py."
    )

JDBC_PROPS = {
    "user": "postgres",
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

ENVELOPE_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("instrument_id", StringType()),
    StructField("event_time_utc", StringType()),
    StructField("ingest_time_utc", StringType()),
    StructField("quality_flag", IntegerType()),
    StructField("payload", MapType(StringType(), StringType())),
])


# ============================================================
# Spark setup
# ============================================================

def create_spark() -> SparkSession:
    """Create the Spark session used by the streaming ingest job.

    The Kafka connector and PostgreSQL JDBC driver are declared as Spark packages
    because they are JVM-side integrations loaded by Spark executors, not Python
    packages imported by this file.

    Returns:
        SparkSession: Configured session named `oil-kzt-ingest`.

    Example:
        >>> spark = create_spark()
        >>> spark.conf.get("spark.sql.shuffle.partitions")
        '4'

    Notes:
        - Four shuffle partitions is small on purpose for a local/container demo.
        - The app name is useful when identifying this stream in Spark logs or UI.
    """
    return (
        SparkSession.builder
        .appName("oil-kzt-ingest")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


# ============================================================
# Micro-batch sink functions
# ============================================================

def write_oil(batch_df, batch_id):
    """Write one Spark micro-batch of oil observations to TimescaleDB.

    Spark calls this function once per trigger via `foreachBatch`. Using a normal
    batch DataFrame write here is simpler than implementing a custom streaming JDBC
    sink, and the checkpoint records which Kafka offsets have already been passed
    into this function.

    Args:
        batch_df (pyspark.sql.DataFrame): Micro-batch parsed from MarketEnvelope JSON.
            Expected columns include `instrument_id`, timestamp strings, `source`,
            `quality_flag`, and `payload`; shape is one row per Kafka message.
        batch_id (int): Monotonic Spark micro-batch identifier. It is available for
            idempotency or logging, but this implementation does not use it.

    Returns:
        None: Rows are appended to `md.raw_oil_prices` as a side effect.

    Example:
        >>> # Spark invokes this through raw.writeStream.foreachBatch(write_oil).
        >>> # A batch row with instrument_id='brent' and payload={'price': '82.1'}
        >>> # becomes one row in md.raw_oil_prices with price=82.1.

    Notes:
        - Only Brent and WTI are written to the oil table; other instruments are
          ignored by this sink.
        - `payload` is a string map, so price is explicitly cast before writing.
        - Empty batches are skipped to avoid unnecessary JDBC connections.
    """
    if batch_df.isEmpty():
        return
    # Filtering inside the sink lets both Kafka topics share one parsed stream while
    # still landing each instrument family in the table shaped for that family.
    df = batch_df.filter(col("instrument_id").isin("brent", "wti"))
    df = df.select(
        # Timescale hypertables need a real timestamp column for time partitioning.
        to_timestamp(col("event_time_utc")).alias("event_time_utc"),
        col("instrument_id"),
        col("payload")["price"].cast(DoubleType()).alias("price"),
        col("source"),
        col("quality_flag"),
        to_timestamp(col("ingest_time_utc")).alias("ingest_time_utc"),
    )
    df.write.jdbc(JDBC_URL, "md.raw_oil_prices", mode="append", properties=JDBC_PROPS)


def write_fx(batch_df, batch_id):
    """Write one Spark micro-batch of USD/KZT FX observations to TimescaleDB.

    This mirrors `write_oil`, but targets the FX table and extracts `rate` from the
    envelope payload. Keeping a separate sink keeps table schemas explicit even
    though the raw Kafka envelope is shared.

    Args:
        batch_df (pyspark.sql.DataFrame): Micro-batch parsed from MarketEnvelope JSON.
            Expected columns include `instrument_id`, timestamp strings, `source`,
            `quality_flag`, and `payload`; shape is one row per Kafka message.
        batch_id (int): Monotonic Spark micro-batch identifier supplied by Spark.
            Currently unused because this sink relies on checkpointed Kafka offsets.

    Returns:
        None: Rows are appended to `md.raw_fx_rates` as a side effect.

    Example:
        >>> # Spark invokes this through raw.writeStream.foreachBatch(write_fx).
        >>> # A row with instrument_id='usdkzt' and payload={'rate': '475.2'}
        >>> # becomes one row in md.raw_fx_rates with rate=475.2.

    Notes:
        - The sink accepts only `usdkzt`; adding another FX pair requires either a
          broader filter or another table contract.
        - `to_timestamp` assumes Spark can parse the producer's ISO-like datetime.
    """
    if batch_df.isEmpty():
        return
    # The envelope keeps payload flexible, but the relational target needs fixed
    # columns, so each sink projects only the fields relevant to its table.
    df = batch_df.filter(col("instrument_id") == "usdkzt")
    df = df.select(
        to_timestamp(col("event_time_utc")).alias("event_time_utc"),
        col("instrument_id"),
        col("payload")["rate"].cast(DoubleType()).alias("rate"),
        col("source"),
        col("quality_flag"),
        to_timestamp(col("ingest_time_utc")).alias("ingest_time_utc"),
    )
    df.write.jdbc(JDBC_URL, "md.raw_fx_rates", mode="append", properties=JDBC_PROPS)


def main():
    """Start the Kafka-to-TimescaleDB streaming ingest queries.

    The function builds one parsed streaming DataFrame from both raw topics, then
    attaches two independent `foreachBatch` sinks. Both sinks consume the same
    source stream but maintain separate checkpoint locations so Spark can recover
    each output independently after a restart.

    Returns:
        None: Blocks while the streaming queries are active.

    Example:
        >>> # From a container with Spark, Kafka, and TimescaleDB reachable:
        >>> # python src/streaming/ingest_job.py

    Notes:
        - `startingOffsets='earliest'` is useful for replay/demo workflows because
          the job can ingest already-published Kafka records on first startup.
        - `maxOffsetsPerTrigger` caps micro-batch size to keep local JDBC writes
          predictable.
    """
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", "raw.oil_prices,raw.fx_rates")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
        # Kafka values arrive as bytes; Spark needs a string before JSON parsing.
        .select(from_json(col("value").cast("string"), ENVELOPE_SCHEMA).alias("e"))
        # Flatten the parsed envelope so sink functions can use normal columns.
        .select("e.*")
    )

    oil_query = (
        raw.writeStream
        .foreachBatch(write_oil)
        .option("checkpointLocation", "/tmp/checkpoints/oil")
        .trigger(processingTime="10 seconds")
        .start()
    )

    fx_query = (
        raw.writeStream
        .foreachBatch(write_fx)
        .option("checkpointLocation", "/tmp/checkpoints/fx")
        .trigger(processingTime="10 seconds")
        .start()
    )

    oil_query.awaitTermination()
    fx_query.awaitTermination()


if __name__ == "__main__":
    main()
