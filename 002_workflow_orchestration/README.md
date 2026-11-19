# NYC Taxi Data Engineering & Workflow Orchestration (Kestra)

This repository contains the end-to-end workflow orchestration project for **Module 2 of the DataTalksClub Data Engineering Zoomcamp**. It demonstrates how to orchestrate scalable, resilient, and idempotent data pipelines using **[Kestra](https://kestra.io/)**, moving data from source APIs to both a local data warehouse (**PostgreSQL**) and modern cloud data warehouse infrastructure (**Google Cloud Storage & BigQuery**).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites & Dependencies](#prerequisites--dependencies)
- [Quick Start](#quick-start)
  - [1. Environment Setup](#1-environment-setup)
  - [2. Start Docker Compose](#2-start-docker-compose)
  - [3. Configure pgAdmin](#3-configure-pgadmin)
- [Direct Python Ingestion (`ingest_data.py`)](#direct-python-ingestion-ingest_datapy)
- [Kestra Architecture & Configuration](#kestra-architecture--configuration)
  - [Kestra OSS Secrets & `.env_encoded`](#kestra-oss-secrets---env_encoded)
  - [Kestra Key-Value (KV) Store](#kestra-key-value-kv-store)
- [Workflows Breakdown (`flows/`)](#workflows-breakdown-flows)
  - [1. Foundations & Testing](#1-foundations--testing)
  - [2. Local PostgreSQL Pipelines](#2-local-postgresql-pipelines)
  - [3. GCP Infrastructure Setup](#3-gcp-infrastructure-setup)
  - [4. Google Cloud Platform Pipelines](#4-google-cloud-platform-pipelines)
- [Idempotency & Data Modeling](#idempotency--data-modeling)
- [Execution Guide](#execution-guide)
  - [Manual Execution](#manual-execution)
  - [Backfill Executions](#backfill-executions)
- [Troubleshooting & Common Pitfalls](#troubleshooting--common-pitfalls)
- [Security Best Practices](#security-best-practices)

---

## Architecture Overview

```
                      ┌──────────────────────────────────────────────┐
                      │    NYC Taxi TLC Data (GitHub Releases)       │
                      │  - yellow_tripdata_{year}-{month}.csv.gz     │
                      │  - green_tripdata_{year}-{month}.csv.gz      │
                      │  - taxi_zone_lookup.csv                      │
                      └──────────────────────┬───────────────────────┘
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       │                                           │
                       ▼                                           ▼
          [ Local Host Pipeline ]                      [ Kestra Orchestrator ]
         uv run python ingest_data.py                    (Docker Compose)
                       │                                           │
                       │                       ┌───────────────────┴───────────────────┐
                       │                       │                                       │
                       ▼                       ▼                                       ▼
            ┌─────────────────────┐ ┌─────────────────────┐         ┌─────────────────────────────────────┐
            │ PostgreSQL (Docker) │ │ PostgreSQL (Docker) │         │       Google Cloud Platform         │
            │  DB: ny_taxi        │ │  DB: ny_taxi        │         │                                     │
            │  Port: 5432         │ │  Staging -> MERGE   │         │  1. Extract / Download              │
            └──────────┬──────────┘ └──────────┬──────────┘         │  2. Upload to GCS Bucket            │
                       │                       │                    │  3. BigQuery External Table         │
                       ▼                       ▼                    │  4. BigQuery Staging Table (MD5 ID) │
            ┌─────────────────────────────────────────┐             │  5. BigQuery Partitioned Table      │
            │            pgAdmin 4 (Docker)           │             │     (MERGE INTO target table)       │
            │            Port: 8085                   │             └─────────────────────────────────────┘
            └─────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Version / Notes |
| --- | --- | --- |
| **Workflow Orchestrator** | [Kestra](https://kestra.io/) | `v1.1` (Standalone Server in Docker) |
| **Orchestrator Backend** | PostgreSQL | `18` (Dedicated metadata DB: `kestra`) |
| **Local Data Warehouse** | PostgreSQL | `18` (Target DB: `ny_taxi`) |
| **Database UI** | pgAdmin 4 | Exposed on port `8085` |
| **Cloud Storage** | Google Cloud Storage (GCS) | Regional bucket for raw CSV/GZ files |
| **Cloud Data Warehouse** | Google Cloud BigQuery | Partitioned tables with deduplicated `MERGE` |
| **Language & Environment** | Python & [uv](https://github.com/astral-sh/uv) | Python `>=3.13`, fast package management |
| **Data Libraries** | Pandas, SQLAlchemy, PyArrow, psycopg | High-performance chunked data loading |

---

## Repository Structure

```text
.
├── docker-compose.yml          # Docker composition: Kestra, Kestra DB, Postgres warehouse, pgAdmin
├── ingest_data.py              # CLI ingestion utility for direct host-to-Postgres imports
├── pyproject.toml              # Project dependencies and tool configurations
├── uv.lock                     # Deterministic dependency lockfile
├── .env_encoded                # Base64-encoded GCP service account secret (git-ignored)
└── flows/                      # Kestra workflow definitions (YAML)
    ├── 01_hello_world.yaml                    # Kestra basics: inputs, variables, concurrency, logs
    ├── 02_python.yaml                         # Containerized Python script runner in Kestra
    ├── 03_getting_started_data_pipeline.yaml  # HTTP Download -> Python transform -> DuckDB query
    ├── 04_postgres_taxi.yaml                  # Local Postgres: Manual parameterized ELT
    ├── 05_postgres_taxi_scheduled.yaml        # Local Postgres: Cron scheduled + backfill ELT
    ├── 06_gcp_kv.yaml                         # Bootstrap Kestra KV store with GCP variables
    ├── 07_gcp_setup.yaml                      # Infrastructure-as-Code: GCS bucket & BQ dataset
    ├── 08_gcp_taxi.yaml                       # GCP: Manual parameterized ELT to GCS & BigQuery
    ├── 09_gcp_taxi_scheduled.yaml             # GCP: Cron scheduled + backfill ELT (uncompressed CSV)
    └── 10_gcp_taxi_csv_gz.yaml                # GCP: Optimized ELT directly streaming .csv.gz
```

---

## Prerequisites & Dependencies

1. **Docker & Docker Compose**: Ensure Docker engine is installed and running (`docker compose version`).
2. **uv (Python Package Manager)**: Install `uv` via:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
3. **Google Cloud Platform Account**:
   - Active GCP project with billing enabled.
   - Enabled APIs: **Cloud Storage API** and **BigQuery API**.
   - A Service Account with roles:
     - `Storage Admin` (`roles/storage.admin`)
     - `BigQuery Admin` (`roles/bigquery.admin`)
   - Downloaded Service Account Key in JSON format.

---

## Quick Start

### 1. Environment Setup

Clone this repository and create the Python virtual environment:

```bash
# Sync Python dependencies
uv sync

# Ensure psycopg binary driver is installed for local scripts
uv add "psycopg[binary]"
```

For GCP authentication in Kestra OSS, encode your GCP Service Account JSON into `.env_encoded`:

```bash
echo "SECRET_GCP_CREDS=$(base64 -w 0 /path/to/your-gcp-key.json)" > .env_encoded
```

> [!WARNING]
> Ensure `.env_encoded` and all `*.json` key files are included in your `.gitignore` to prevent credential exposure.

### 2. Start Docker Compose

Launch the local infrastructure stack:

```bash
docker compose up -d
```

Verify all containers are up and healthy:

```bash
docker compose ps
```

| Service | Internal Hostname | Host Port | Credentials / Purpose |
| --- | --- | --- | --- |
| **Kestra Web UI** | `kestra` | `http://localhost:8080` | `admin@kestra.io` / `Admin1234!` |
| **pgAdmin 4** | `pgadmin` | `http://localhost:8085` | `admin@admin.com` / `root` |
| **PostgreSQL Warehouse** | `pgdatabase` | `localhost:5432` | User: `root`, Pass: `root`, DB: `ny_taxi` |
| **Kestra Metadata DB** | `kestra_postgres` | *(Internal only)* | User: `kestra`, DB: `kestra` |

### 3. Configure pgAdmin

1. Open your browser at `http://localhost:8085`.
2. Log in using `admin@admin.com` / `root`.
3. Right click **Servers** → **Register** → **Server**:
   - **General Tab**: Name = `Local Taxi Postgres`
   - **Connection Tab**:
     - Host name / address: `pgdatabase` *(Use the Docker service name, not localhost)*
     - Port: `5432`
     - Maintenance database: `ny_taxi`
     - Username: `root`
     - Password: `root`
4. Click **Save** to view tables, schemas, and execute SQL queries.

---

## Direct Python Ingestion (`ingest_data.py`)

If you want to quickly populate your local PostgreSQL warehouse without running Kestra flows or building custom Docker ingestion images, use the unified script [ingest_data.py](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/ingest_data.py).

### Features
- Ingests **Yellow Taxi** (`.csv.gz`), **Green Taxi** (`.parquet`), and **Taxi Zone Lookup** (`.csv`).
- Streams data in memory-safe chunks (`--chunksize 100000`).
- Auto-handles missing types, enforces date parsing, and uses `if_exists="replace"` for idempotency.
- Reads password securely from environment variable or interactive prompt (no hardcoded credentials).

### Usage

```bash
# Ingest all datasets into local PostgreSQL:
PGPASSWORD=root uv run python ingest_data.py

# Ingest only taxi zones and green taxi data:
PGPASSWORD=root uv run python ingest_data.py --datasets zones green

# Ingest specific Yellow Taxi month:
PGPASSWORD=root uv run python ingest_data.py --datasets yellow --yellow-year 2021 --yellow-month 2

# Inspect all available options:
uv run python ingest_data.py --help
```

---

## Kestra Architecture & Configuration

Kestra runs in standalone mode using PostgreSQL for its internal metadata and execution queue.

### Kestra OSS Secrets & `.env_encoded`
In Kestra Community (OSS), secrets referenced via `{{ secret('SECRET_NAME') }}` are resolved from environment variables named `SECRET_<NAME>`.

Your [docker-compose.yml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/docker-compose.yml#L90-L91) attaches `.env_encoded` to the `kestra` service:
```yaml
    env_file:
      - .env_encoded
```
When Kestra executes `{{ secret('GCP_CREDS') }}`, it reads `SECRET_GCP_CREDS`, decodes the base64 string, and authenticates the GCP client.

### Kestra Key-Value (KV) Store
The KV Store allows flows in the `zoomcamp` namespace to share non-sensitive environment variables without hardcoding them into multiple YAML files.

The required keys are:
- `GCP_PROJECT_ID`: Your GCP project ID (e.g. `data-engineering-gcp-514411`).
- `GCP_LOCATION`: Target region (e.g. `us-central1` or `europe-west2`).
- `GCP_BUCKET_NAME`: Globally unique bucket name (e.g. `zoomcamp-zackmendel`).
- `GCP_DATASET`: Target BigQuery dataset (e.g. `zoomcamp`).

You can populate these keys in the Kestra UI under **Namespaces → zoomcamp → KV Store**, or by running the flow [06_gcp_kv.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/06_gcp_kv.yaml).

---

## Workflows Breakdown (`flows/`)

All workflows are located in the [flows/](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/) folder and belong to the `zoomcamp` namespace.

```
flows/
├── 01_hello_world.yaml
├── 02_python.yaml
├── 03_getting_started_data_pipeline.yaml
├── 04_postgres_taxi.yaml
├── 05_postgres_taxi_scheduled.yaml
├── 06_gcp_kv.yaml
├── 07_gcp_setup.yaml
├── 08_gcp_taxi.yaml
├── 09_gcp_taxi_scheduled.yaml
└── 10_gcp_taxi_csv_gz.yaml
```

### 1. Foundations & Testing
- **[01_hello_world.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/01_hello_world.yaml)**: Demonstrates fundamental Kestra building blocks including flow inputs, variable rendering (`{{ render(...) }}`), task outputs (`{{ outputs.generate_output.value }}`), sleep steps, and concurrency limits.
- **[02_python.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/02_python.yaml)**: Runs a containerized Python script using `io.kestra.plugin.scripts.runner.docker.Docker`. Installs packages dynamically and passes outputs back to Kestra via `Kestra.outputs()`.
- **[03_getting_started_data_pipeline.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/03_getting_started_data_pipeline.yaml)**: An ETL pipeline downloading JSON products via HTTP, parsing and filtering columns with an Alpine Python container, and querying the result with an in-memory **DuckDB** instance.

### 2. Local PostgreSQL Pipelines
- **[04_postgres_taxi.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/04_postgres_taxi.yaml)**:
  - **Type**: Manual parameterized flow (inputs: `taxi`, `year`, `month`).
  - **Extract**: Downloads gzipped CSV from GitHub and pipes through `gunzip`.
  - **Load**: Uses `io.kestra.plugin.jdbc.postgresql.CopyIn` for high-throughput batch loading into a staging table (`public.<taxi>_tripdata_staging`).
  - **Transform & Deduplicate**: Computes `unique_row_id` using MD5 hash over primary attributes, then executes SQL `MERGE INTO public.<taxi>_tripdata`.
- **[05_postgres_taxi_scheduled.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/05_postgres_taxi_scheduled.yaml)**:
  - **Type**: Automated scheduled flow using `io.kestra.plugin.core.trigger.Schedule` (runs on the 1st of every month).
  - Uses `trigger.date` to determine the target dataset partition.
  - Supports full historical **Backfills** from the Kestra UI.

### 3. GCP Infrastructure Setup
- **[06_gcp_kv.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/06_gcp_kv.yaml)**: One-click setup flow that sets all necessary GCP configuration keys into Kestra's KV store.
- **[07_gcp_setup.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/07_gcp_setup.yaml)**: Declarative Infrastructure as Code (IaC). Creates the Cloud Storage bucket (`io.kestra.plugin.gcp.gcs.CreateBucket`) and the BigQuery dataset (`io.kestra.plugin.gcp.bigquery.CreateDataset`) with `ifExists: SKIP`.

### 4. Google Cloud Platform Pipelines
- **[08_gcp_taxi.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/08_gcp_taxi.yaml)**:
  - **Type**: Manual parameterized ELT to GCP.
  - Downloads `.csv.gz`, uncompresses, and uploads raw `.csv` to GCS bucket (`gs://<bucket>/<file>`).
  - Creates the target partitioned BigQuery table (`PARTITION BY DATE(pickup_datetime)`).
  - Creates a BigQuery **External Table** referencing the GCS CSV file.
  - Creates a monthly staging table with generated MD5 `unique_row_id`.
  - Merges records into the main partitioned table with `WHEN NOT MATCHED THEN INSERT`.
- **[09_gcp_taxi_scheduled.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/09_gcp_taxi_scheduled.yaml)**:
  - **Type**: Scheduled / Backfill ELT to GCP for uncompressed CSV data using `trigger.date`.
- **[10_gcp_taxi_csv_gz.yaml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/flows/10_gcp_taxi_csv_gz.yaml)**:
  - **Type**: Bandwidth-optimized scheduled / backfill ELT.
  - Skips decompression on the runner and uploads raw `.csv.gz` directly to GCS.
  - BigQuery reads compressed `.csv.gz` directly via external table definitions, significantly saving disk I/O and network transfer.

---

## Idempotency & Data Modeling

A crucial requirement for production data pipelines is **idempotency**: running a workflow multiple times with the same inputs must yield the same result without duplicating records.

### 1. Hash-based Unique Identifiers
NYC Taxi records lack natural single-column primary keys. The workflows compute a deterministic surrogate key using MD5 hashing across distinguishing trip attributes:

```sql
MD5(CONCAT(
  COALESCE(CAST(VendorID AS STRING), ""),
  COALESCE(CAST(pickup_datetime AS STRING), ""),
  COALESCE(CAST(dropoff_datetime AS STRING), ""),
  COALESCE(CAST(PULocationID AS STRING), ""),
  COALESCE(CAST(DOLocationID AS STRING), "")
)) AS unique_row_id
```

### 2. Atomic `MERGE` Operations
Instead of naive `INSERT INTO` statements, data is loaded into staging tables and merged into target production tables:

```sql
MERGE INTO `project.dataset.yellow_tripdata` T
USING `project.dataset.yellow_tripdata_2021_01` S
ON T.unique_row_id = S.unique_row_id
WHEN NOT MATCHED THEN
  INSERT (...) VALUES (...);
```

If a pipeline is re-run, existing rows match `T.unique_row_id = S.unique_row_id` and are skipped.

### 3. BigQuery Partitioning
Target BigQuery tables are partitioned by pickup date:
```sql
PARTITION BY DATE(tpep_pickup_datetime);
```
Partitioning limits data scanned during analytical queries, reducing costs and accelerating query performance.

---

## Execution Guide

### Manual Execution
1. Navigate to `http://localhost:8080`.
2. Go to **Flows** → Select `04_postgres_taxi` or `08_gcp_taxi`.
3. Click the purple **Execute** button in the upper right.
4. Select your parameters (e.g. `taxi: green`, `year: 2020`, `month: 01`) and run.
5. Inspect real-time execution logs, Gantt chart, and task outputs in the **Executions** tab.

### Backfill Executions
For scheduled flows (`05_postgres_taxi_scheduled`, `09_gcp_taxi_scheduled`, `10_gcp_taxi_csv_gz`):
1. Open the flow in the Kestra UI and click the **Triggers** tab.
2. Under the relevant schedule (e.g., `green_schedule`), click **Backfill executions**.
3. Select the date window (e.g., `2020-01-01 00:00:00` to `2020-12-01 00:00:00`).
4. Add label `backfill: true` to easily filter backfill executions in the UI.
5. Click **Execute Backfill**. Kestra will queue and execute one flow per month in chronological order.

---

## Troubleshooting & Common Pitfalls

### 1. `PebbleException: Unable to find trigger`
- **Cause**: Clicking **Execute** manually on a scheduled flow (`05`, `09`, `10`) that uses `{{ trigger.date }}`. In manual runs, `trigger` is null.
- **Solution**: Run scheduled flows via **Triggers → Backfill**, or provide a fallback expression:
  ```yaml
  {{ (trigger is defined ? trigger.date : execution.startDate) | date('yyyy-MM') }}
  ```

### 2. `PebbleException: The key '...' does not exist in namespace 'zoomcamp'`
- **Cause**: Passing literal values into `{{ kv('...') }}` instead of the key name (e.g., `{{ kv('my-project-id') }}` instead of `{{ kv('GCP_PROJECT_ID') }}`).
- **Solution**: Run flow `06_gcp_kv` or add keys under **Namespaces → zoomcamp → KV Store**.

### 3. `ImportError: no pq wrapper available (libpq not found)`
- **Cause**: Python `psycopg` v3 driver requires either the system C library `libpq` or the pre-compiled binary package.
- **Solution**: Install `psycopg[binary]`:
  ```bash
  uv add "psycopg[binary]"
  ```

### 4. pgAdmin cannot connect to `localhost:5432`
- **Cause**: pgAdmin runs inside a Docker container network; `localhost` refers to the container itself.
- **Solution**: When registering the database server in pgAdmin, use the Docker Compose service name: **`pgdatabase`**.

### 5. Docker Compose YAML Schema Errors
- In [docker-compose.yml](file:///home/zackmendel/projects/analytics_projects/002_workflow_orchestration/docker-compose.yml), Kestra application configuration belongs inside the multi-line `KESTRA_CONFIGURATION:` string under `environment:`. Top-level container properties like `env_file:` and `ports:` must be indented at the service level (`services.kestra`).

---

## Security Best Practices

- **Never commit `.env_encoded` or GCP service account `.json` keys to GitHub.**
- In production deployments:
  - Restrict the GCP Service Account permissions using the Principle of Least Privilege.
  - Enable SSL/mTLS for PostgreSQL connections.
  - Run Kestra in rootless mode by following the official [Kestra Rootless Podman/Docker Guide](https://kestra.io/docs/installation/podman-compose).
