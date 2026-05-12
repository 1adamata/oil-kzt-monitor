# oil-kzt-monitor

Real-time monitoring system for the relationship between global crude oil prices (Brent/WTI) and the Kazakhstani Tenge (USD/KZT exchange rate).

## What this is

Kazakhstan derives ~50% of export revenues from hydrocarbons. Oil price shocks propagate into the Tenge via the Dutch Disease channel — when oil rises, petrodollars flow in and the Tenge strengthens; when oil collapses, the Tenge follows. This system tracks that relationship in near real-time, computes rolling correlation and anomaly metrics, and fires alerts when the relationship breaks down.

**The breakdown is the signal.** When oil and KZT decouple — as they did during the 2008 financial crisis, the 2014–2015 oil crash, the April 2020 COVID collapse, and the February 2026 Middle East conflict — that decoupling is detectable before it becomes obvious in the news.

## What it demonstrates

- End-to-end data engineering: ingestion → streaming → storage → dashboards
- Quantitative analytics: log returns, rolling Pearson correlation, beta, spread z-score
- Anomaly detection: CUSUM and changepoint detection on 25 years of daily data
- ML extensions: XGBoost spread forecaster, Random Forest regime classifier
- Production-style architecture running entirely local in Docker

Next section — the stack and architecture:


## Stack

| Layer | Tool | Version |
|---|---|---|
| Message broker | Redpanda (Kafka-compatible) | v26.1.5 |
| Stream processing | Apache Spark / PySpark | 4.1.1 |
| Database | TimescaleDB on PostgreSQL | 2.26.3 / pg17 |
| Dashboards | Grafana OSS | 13.0.1 |
| Language | Python | 3.13 |
| Package manager | uv | 0.7.8 |
| Container orchestration | Docker Compose | 2.40.3 |

## Architecture

```
Data Sources (EIA API, NBK RSS)
        ↓
Python Producers (replay or live polling)
        ↓
Redpanda (Kafka-compatible broker)
        ↓
Spark Structured Streaming (ingest job)
        ↓
TimescaleDB (5 hypertables)
        ↓
Grafana (live dashboards) + Python analytics jobs
```

**Two operating modes:**
- **Replay mode** — replays 25 years of historical Brent/WTI and USD/KZT data through the pipeline at accelerated speed
- **Live mode** — polls EIA and NBK daily for fresh prices and publishes to Kafka

## Data sources

- **Oil prices:** EIA v2 API (Brent `EPCBRENT`, WTI `EPCWTI`) — free, daily, back to 1987
- **USD/KZT:** National Bank of Kazakhstan RSS endpoint — free, daily, no API key required
- **Historical KZT:** Kaggle dataset (2000–2024) for corpus backfill

## Setup

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- uv (`pip install uv`)
- EIA API key (free at https://www.eia.gov/opendata/register.php)

### 1. Clone and configure

```bash
git clone https://github.com/1adamata/oil-kzt-monitor
cd oil-kzt-monitor
cp .env.example .env
# Edit .env and add your EIA_API_KEY
```

### 2. Start the stack

```bash
make up
make ps  # wait until all services are healthy
```

Services:
- Redpanda: `localhost:19092`
- TimescaleDB: `localhost:5432`
- Grafana: `http://localhost:3000` (admin/changeme)
- Spark UI: `http://localhost:8080`

### 3. Create Kafka topics

```bash
docker exec -it redpanda-0 rpk topic create raw.oil_prices raw.fx_rates curated.aligned_bars features.metrics alerts.signals dlq.ingestion --partitions 3 --replicas 1
```

### 4. Build the corpus

```bash
uv run python src/producers/fetch_corpus.py
```

Downloads 25 years of Brent, WTI, and USD/KZT data into `data/corpus/`.

### 5. Run the replay producer

```bash
uv run python src/producers/replay_producer.py
```

### 6. Run the Spark ingest job

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --master local[2] \
  --conf "spark.jars.ivy=/tmp/.ivy" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3" \
  /opt/spark-apps/src/streaming/ingest_job.py
```

### 7. Run the analytics pipeline

```bash
uv run python src/analytics/align_bars.py
uv run python src/analytics/compute_metrics.py
uv run python src/analytics/detect_anomalies.py
```

### 8. Train the ML models

```bash
uv run python src/ml/train_models.py
```

### 9. Open Grafana

Go to `http://localhost:3000` and open the **OIL-KZT Monitor** dashboard.

## Key findings (25 years of data)

The system detected 41 anomaly alerts across 25 years, clustering around known macroeconomic events:

| Year | Event | Alerts |
|---|---|---|
| 2001 | Post-9/11 oil shock | 3 |
| 2008 | Global financial crisis | 2 |
| 2014–2015 | Oil price collapse ($115→$27), two KZT devaluations | 6 |
| 2020 | COVID crash, WTI briefly negative | 14 |
| 2022 | Russia-Ukraine war, commodity shock | 2 |
| 2026 | US-Israel strikes on Iran, Strait of Hormuz disruption | 2 |

### Correlation (Pearson r)
Average correlation is near zero (+0.013 to +0.021) but the range tells the real story:
- Brent 30d: -0.558 to +0.577
- WTI 30d: -0.616 to +0.628

The relationship swings between strongly negative (oil-driven Tenge strengthening) and strongly positive (crisis decoupling). The near-zero average reflects NBK intervention smoothing out the daily signal.

### Beta
Average beta is effectively zero (0.002–0.003), confirming the administered nature of the NBK rate dampens proportional responses. Crisis periods produce negative beta spikes down to -1.013 (Brent 30d) — meaning oil and KZT moved strongly in opposite directions at those moments.

### Spread Z-Score
The spread reached extremes of -6.925 and +4.874 during crisis periods. The CUSUM detector uses a threshold of 1.5 and trigger of 4.0, which proved well-calibrated — firing only during genuine macro events rather than routine volatility.

## Project structure

```
oil-kzt-monitor/
├── src/
│   ├── common/schemas.py          # Pydantic MarketEnvelope schema
│   ├── producers/
│   │   ├── fetch_corpus.py        # Downloads 25y of oil + FX data
│   │   ├── replay_producer.py     # Replays corpus through Kafka
│   │   └── live_producer.py       # Daily polling from EIA + NBK
│   ├── streaming/
│   │   └── ingest_job.py          # Spark Structured Streaming job
│   ├── analytics/
│   │   ├── align_bars.py          # Aligns oil + FX into daily bars
│   │   ├── compute_metrics.py     # Rolling correlation, beta, z-score
│   │   └── detect_anomalies.py    # CUSUM + changepoint detection
│   └── ml/
│       └── train_models.py        # XGBoost + Random Forest training
├── docker/
│   ├── docker-compose.yml
│   ├── timescaledb/init-sql/      # Schema + hypertables
│   └── grafana/
│       ├── provisioning/          # Auto-configured datasource
│       └── dashboards/            # OIL-KZT Monitor dashboard
├── data/corpus/                   # Parquet files (gitignored)
├── Makefile                       # up, down, psql, rpk, help
└── .env.example                   # Environment variable template
```

## Makefile targets

```bash
make up          # Start all services
make down        # Stop all services
make ps          # Show service status
make psql        # Open psql shell
make rpk         # Open rpk shell (Redpanda)
make logs        # Tail all logs
make nuke        # Wipe all volumes (fresh start)
```
## Resume bullets

**Data Engineering:**
- Built an end-to-end streaming pipeline (Python producers → Redpanda/Kafka → Spark Structured Streaming → TimescaleDB hypertables → Grafana) processing 25 years of daily oil and FX data with event-time alignment and data quality flags
- Designed a Pydantic-validated message envelope schema with event-time vs ingest-time separation, enabling reliable lag detection across producers and consumers

**Quantitative Analytics:**
- Implemented rolling Pearson correlation, beta regression, and spread z-score across 30d/60d horizons on aligned log returns, detecting oil-KZT relationship breakdowns during 6 major macroeconomic events (2008, 2014–15, 2020, 2022, 2026)
- Applied CUSUM and changepoint detection on 25 years of spread data, generating 41 alerts that map to real geopolitical events with no manual labeling

**ML Engineering:**
- Trained XGBoost spread forecaster and Random Forest regime classifier on engineered features (lagged z-scores, rolling correlation, momentum) achieving MAE of 0.78 on next-day spread prediction
- Identified class imbalance limitation (~2% anomalous days) and documented SMOTE oversampling as a production improvement path

## Acknowledgements

- Oil price data: [U.S. Energy Information Administration](https://www.eia.gov/opendata/)
- USD/KZT data: [National Bank of Kazakhstan](https://nationalbank.kz)
- Historical KZT data: [Kaggle — iskakovyerassyl](https://www.kaggle.com/datasets/iskakovyerassyl/usd-kzt-eur-kzt-rub-kzt-and-cny-kzt-20002024)
- Economic context: [IEA Oil Market Report, March 2026](https://www.iea.gov/reports/oil-market-report-march-2026)

## License

MIT — see [LICENSE](LICENSE)
