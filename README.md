# News Sentiment Analysis – End‑to‑End Data Engineering Project
<img width="1268" height="271" alt="image" src="https://github.com/user-attachments/assets/f5f6e1b0-2b2c-4ef4-9346-f77b470918c8" />

This repository contains an automated pipeline that ingests RSS feeds from major Indian English news websites every 30 minutes betweem 10am to 10pm IST, lands them in a **data lake** (MinIO/S3) using **Bronze → Silver → Gold** layers, enriches them with **sentiment analysis (FinBERT)**, and then loads curated data into **PostgreSQL** for **Metabase** dashboards.

> Key goals: reliable ingestion, schema standardization, NLP enrichment, **idempotent/deduplicated** loads at every stage, and easy BI consumption.

---

## Architecture at a glance

```mermaid
flowchart LR
  subgraph Source[News Sources]
    A[RSS Feeds: Hindustan Times, TOI, The Hindu, Indian Express, India Today, NDTV, News18, Firstpost, DNA, Scroll, OpIndia]
  end

  A --> B[Bronze (raw RSS → Parquet)
  • Unique ID
  • Partitioned by load_date & source]

  B --> C[Silver (clean/typed)
  • Normalize schema
  • Parse dates & tags]

  C --> D[Gold (enriched)
  • Text selection for NLP
  • FinBERT sentiment + score]

  D --> E[(PostgreSQL)
  • `news_data` table
  • Incremental upserts]

  E --> F[Metabase Dashboards
  • Sentiment distribution
  • Headlines per site
  • Trends]

  classDef store fill:#f6f8fa,stroke:#d0d7de,color:#24292e;
  classDef compute fill:#fff7ed,stroke:#f0abfc,color:#3b0764;
  class B,C,D store; class E store; class F compute
```

---

## Tech stack

* **Python**: `requests`, `feedparser`, `pandas`, `polars`, `duckdb`, `s3fs`
* **Object storage (data lake)**: **MinIO** (S3‑compatible)
* **NLP**: `transformers` → `yiyanghkust/finbert-tone`
* **Warehouse / BI**: **PostgreSQL** → **Metabase**
* **Orchestration**: **Dagster** (scheduled every 30 minutes; linear dependencies)

---

## Repo layout (key file)

* `etl.py` – all functions to extract, transform, enrich and load data

---

## Environment & prerequisites

### 1) Services

* **MinIO** running on `http://localhost:9000` with a bucket for each layer:

  * `news-data-bronze`
  * `news-data-silver`
  * `news-data-gold`
* **PostgreSQL** running locally (or in Docker) and reachable from Python
* **Metabase** connected to the PostgreSQL database

> The code sets DuckDB S3 configs to `endpoint=localhost:9000`, `access_key_id=admin`, `secret_access_key=omkarPawar` (see `etl.py`). Replace with your own credentials in production.

### 2) Python packages

```bash
pip install -r requirements.txt
```

Minimal `requirements.txt` (adjust as needed):

```
requests
feedparser
pandas
polars
duckdb
s3fs
minio
transformers
torch        # CPU is fine
python-dotenv
SQLAlchemy
psycopg2-binary
```

### 3) Environment variables (`.env`)

```ini
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=username
MINIO_SECRET_KEY=securepassowr
# Example Postgres URI (adjust host/port/db)
POSTGRES_URI=postgresql://admin:omkarPawar@localhost:5432/postgres
```

> The code currently reads `MINIO_*` from `.env`, and also sets DuckDB S3 settings inline. Keep them consistent.

---

## Pipeline functions & logic

### 1) Bronze – `load_rrs_data_in_bronze()`

**Purpose:** Extract raw RSS entries, compute a stable `unique_id`, and write **partitioned Parquet** to `s3://news-data-bronze` under `load_date=YYYY-MM-DD/source=<SITE>/part_<ts>.parquet`.

* Extraction uses `requests` + `feedparser` (with retries & UA header).
* DataFrame normalization: `pandas.json_normalize(feed.entries)`.
* **Stable ID**: `unique_id = sha256(title + source)` via `generate_id(title, source)`.
* Columns include (typical): `title, link, summary, published, author, media_content, tags, id, source, unique_id, load_date_time, load_date`.
* **Dedup @ Bronze (idempotent append)**: when the bucket already contains data,

  ```sql
  -- In DuckDB
  SELECT *
  FROM df
  WHERE unique_id NOT IN (
    SELECT unique_id
    FROM read_parquet('s3://news-data-bronze/*/*/*.parquet')
  );
  ```

  Only the new `unique_id`s are saved via `save_parquet_partition()`; existing ones are skipped.

### 2) Silver – `load_to_silver()` + `transform_df_silver(df)`

**Purpose:** Harmonize types & schema, extract useful tag fields, and save partitioned Parquet to `s3://news-data-silver/transform_date=YYYY-MM-DD/part_<ts>.parquet`.

* Type casting: `published → pub_date (datetime)`, `load_date`, `load_date_time` to proper types.
* Null handling: `tags = 'No Tag'` where missing.
* Tag parsing (safe):

  ```python
  term1, term2 = extract_terms_split(tags)
  ```

  using `ast.literal_eval` for inputs that are stringified lists of dicts.
* **Dedup @ Silver:** when silver already has files, select only those rows from **Bronze** whose `unique_id` is **not** already present in **Silver**:

  ```sql
  SELECT {columns}
  FROM read_parquet('s3://news-data-bronze/*/*/*.parquet', union_by_name=True)
  WHERE unique_id NOT IN (
    SELECT unique_id FROM read_parquet('s3://news-data-silver/*/*.parquet')
  );
  ```

### 3) Gold – `load_to_gold()` + `transform_to_gold(df)`

**Purpose:** Enrich the text with **sentiment analysis** and save partitioned Parquet to `s3://news-data-gold/transform_date=YYYY-MM-DD/part_<ts>.parquet`.

* Text selection for analysis:

  ```python
  text_for_analysis = summary if summary not empty and not containing "href" else title
  ```
* Sentiment model:

  ```python
  from transformers import pipeline
  finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
  results = finbert(texts)
  # Adds two columns
  sentiment ∈ {"positive","negative","neutral"}
  score ∈ [0,1]
  ```
* **Dedup @ Gold:** when gold already has files, select only `unique_id`s from **Silver** that are **not** in **Gold** (same `NOT IN` pattern using DuckDB over Parquet).

### 4) PostgreSQL load – `load_to_postgres()`

**Purpose:** Publish the curated Gold dataset to a relational table for BI.

* Reads from `s3://news-data-gold/*/*.parquet` via DuckDB, adjusts a few columns for SQL storage (`pubDate = DATE(pub_date)`), and drops verbose/raw columns (`tags`, `id`, `published`).
* Uses SQLAlchemy engine (example shown in code) to write to table **`news_data`**.
* **Idempotent append to Postgres:**

  * If table **exists**: fetch `unique_id` list from Postgres and only append new rows:

    ```sql
    SELECT *
    FROM read_parquet('s3://news-data-gold/*/*.parquet')
    WHERE unique_id NOT IN (<existing_uid_list_from_postgres>);
    ```

    Then `to_sql("news_data", if_exists="append")`.
  * If table **does not exist**: create with full Gold dataset via `to_sql(..., if_exists="replace")`.

> **Dedup summary:** The same **`unique_id`** flows through all layers. Each stage uses **DuckDB over Parquet** (or Postgres) to filter out seen IDs before writing/appending. This makes the pipeline **safe to re‑run** and **robust to retries**.

---

## Orchestration (Dagster)

A simple linear dependency chain runs **every 30 minutes**:

```python
# pseudo-code sketch (put in a dagster job file, e.g., jobs.py)
from dagster import job, op, ScheduleDefinition
from etl import load_rrs_data_in_bronze, load_to_silver, load_to_gold, load_to_postgres

@op
def bronze():
    load_rrs_data_in_bronze()

@op
def silver():
    load_to_silver()

@op
def gold():
    load_to_gold()

@op
def postgres():
    load_to_postgres()

 #Create a job from all assets
etl_job = define_asset_job("etl_job", selection="*")

etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0,30 10-22 * * *",  # Valid cron expression
    execution_timezone="Asia/Kolkata"
)


defs = Definitions(
    assets=[assets.bronze, assets.silver, assets.gold, assets.postgres],
    schedules=[etl_schedule],
)
```

> You can wire these into your existing Dagster project and use the Dagster web UI/daemon to run them on schedule.

---

## Data model (PostgreSQL `news_data`)

Columns may vary slightly by feed, but common fields include:

* `unique_id` (PK candidate, SHA‑256 of `title+source`)
* `title`, `link`, `source`
* `summary` (cleaned), `author`, `media_content`
* `pub_date` (timestamp), `pubDate` (date), `load_date`, `load_date_time`
* `term1`, `term2` (parsed from tags where available)
* `sentiment` (`positive` | `neutral` | `negative`), `score` (float)

> In Metabase, set a **fingerprint**/index on `unique_id` if you expect large volumes, and consider a composite unique constraint on (`unique_id`).

---

## Metabase: sample SQL for visuals

### 1) Sentiment distribution (today)

```sql
SELECT sentiment, COUNT(*) AS cnt
FROM news_data
WHERE pubDate = CURRENT_DATE
GROUP BY sentiment
ORDER BY cnt DESC;
```

### 2) Headlines per site (with Metabase Field Filter)

```sql
SELECT source,
       COUNT(DISTINCT unique_id) AS headlines
FROM news_data
WHERE {{pub_date}}  -- bind this to the `pubDate` field filter (type: Date)
GROUP BY source
ORDER BY headlines DESC;
```

> In the Metabase SQL editor, add a **Field Filter** variable named `pub_date`, map it to the `pubDate` field, and keep the SQL placeholder `{{pub_date}}` exactly as above.

### 3) Top negative/positive stories by site (last 7 days)

```sql
SELECT source, title, link, pub_date, sentiment, score
FROM news_data
WHERE pubDate >= CURRENT_DATE - INTERVAL '7 days'
  AND sentiment = {{sentiment}}  -- dropdown: 'positive' or 'negative'
ORDER BY score DESC
LIMIT 50;
```

### 4) Trend of daily headlines (last 30 days)

```sql
SELECT pubDate, COUNT(*) AS headlines
FROM news_data
WHERE pubDate >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY pubDate
ORDER BY pubDate;
```

---

## How to run locally (without Dagster)

```bash
python - <<'PY'
from etl import load_rrs_data_in_bronze, load_to_silver, load_to_gold, load_to_postgres
load_rrs_data_in_bronze()
load_to_silver()
load_to_gold()
load_to_postgres()
PY
```

> Ensure MinIO and PostgreSQL are up, and your `.env` is loaded.

---

## Operational notes & best practices

* **Idempotency:** Re‑runs are safe; each stage filters known `unique_id`s before writing/appending.
* **Partitioning:** Each layer is partitioned by a transform/load date folder; use these in DuckDB for efficient reads.
* **Timezone:** DuckDB is configured to `Asia/Kolkata` to keep timestamps consistent with the news cycle.
* **Security:** Move hard‑coded credentials to environment variables; never commit secrets.
* **Performance:** Batched FinBERT inference is used; for larger volumes, consider model quantization or a GPU‑backed inference server.
* **Data quality:** RSS feeds vary; the code falls back to `title` when `summary` is empty/HTML‑ish.

---

## Future improvements

* Upsert logic in Postgres using a **staging table + MERGE** (or `ON CONFLICT DO NOTHING` with `unique_id` unique index)
* Add **source reliability** flags or editorial tags
* Add **language detection**/translation for non‑English feeds
* Add **job metrics** & **alerting** (Dagster sensors, Prometheus/Grafana)
* Use **Iceberg/Delta** instead of raw Parquet for ACID & schema evolution

---

## Troubleshooting

* **Metabase Field Filter** with date: make sure your variable is `{{pub_date}}` and mapped to the `pubDate` column. The `WHERE` clause must come **before** `GROUP BY`.
* **DuckDB S3 errors**: double‑check endpoint, access keys, and `s3_url_style='path'`, `s3_use_ssl=false` when using MinIO locally.
* **Transform errors**: some feeds omit fields—keep `union_by_name=True` in DuckDB reads.

---

## License / data usage

This project consumes publicly available RSS feeds for educational purposes. Review each source’s terms of use before redistribution.


