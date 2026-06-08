# E-Commerce Lakehouse on AWS

A production-grade Lakehouse architecture for an e-commerce platform on AWS. The system ingests raw transactional data from Amazon S3, validates and deduplicates it using PySpark and Delta Lake via AWS Glue, and exposes clean data for analytics through Amazon Athena — all triggered automatically by EventBridge and orchestrated by AWS Step Functions.

**Deployed:** AWS account `834424012278`, region `us-east-1`  
**Infrastructure:** 47 Terraform-managed resources  
**Tests:** 28 pytest tests, 100% passing

---

## Architecture Overview

```
CSV/XLSX Sources
       │  upload to S3 raw zone
       ▼
S3 Raw Zone  ──►  EventBridge Rule  ──►  Step Functions State Machine
                  (S3 Object Created)            │
                                      ┌──────────┼──────────┐
                                      ▼          ▼          ▼
                                   Products   Orders   OrderItems
                                   Glue ETL  Glue ETL  Glue ETL
                                      └──────────┼──────────┘
                                                 │ (all 3 parallel)
                                      ┌──────────┴──────────┐
                                      ▼                     ▼
                               ArchiveRawFiles         S3 DWH Zone
                               (Lambda function)      (Delta Tables)
                                                           │
                                                  Glue Crawler
                                                           │
                                               Glue Data Catalog
                                                           │
                                                      Athena SQL
```

### AWS Services

| Service | Role |
|---------|------|
| **S3** | Storage backbone — 6 buckets across 4 zones |
| **AWS Glue 4.0** | PySpark ETL — validate, deduplicate, merge |
| **Delta Lake 2.4.0** | ACID transactions, upserts, time-travel |
| **Step Functions** | Pipeline orchestration — parallel + sequential |
| **EventBridge** | Event-driven trigger on S3 file arrival |
| **Lambda** | Post-ETL raw file archival |
| **Glue Data Catalog** | Schema registry for Athena |
| **Athena** | SQL analytics over Delta tables |
| **SNS** | Failure alerting to data engineering team |
| **IAM** | Least-privilege roles for each service |
| **Terraform** | Infrastructure as Code |
| **GitHub Actions** | CI/CD — lint → test → terraform → deploy |

---

## Project Structure

```
E-commerce-Lakehouse/
├── etl/
│   ├── jobs/
│   │   ├── etl_products.py          # Products dimension ETL (SCD1 upsert)
│   │   ├── etl_orders.py            # Orders fact ETL
│   │   └── etl_order_items.py       # Order items fact ETL
│   ├── utils/
│   │   ├── spark_session.py         # Spark session factory with Delta config
│   │   ├── s3_utils.py              # S3 read/write/archive helpers
│   │   └── logger.py                # Structured logging
│   └── validation/
│       └── rules.py                 # Validation rules engine (5 rules)
├── lambda/
│   └── archive_raw_files.py         # Moves processed files to archive zone
├── orchestration/
│   └── step_function_definition.json # AWS Step Functions ASL (reference copy)
├── terraform/
│   ├── main.tf                      # Root module — wires all modules
│   ├── variables.tf
│   ├── outputs.tf
│   ├── environments/
│   │   └── dev/terraform.tfvars
│   └── modules/
│       ├── s3/                      # 6 S3 buckets
│       ├── iam/                     # Glue, Step Functions, Lambda, EventBridge roles
│       ├── glue/                    # 3 Glue jobs + crawler + Glue DB
│       ├── step_functions/          # State machine (ASL via templatefile)
│       ├── eventbridge/             # S3 trigger rule
│       └── athena/                  # Athena workgroup
├── config/
│   └── settings.py                  # Centralized configuration and schemas
├── tests/
│   ├── test_etl_products.py
│   ├── test_etl_orders.py
│   ├── test_etl_order_items.py
│   └── test_validation_rules.py
├── scripts/
│   └── deploy_glue_jobs.sh          # Manual deployment helper
├── .github/
│   └── workflows/
│       └── ci_cd.yml                # GitHub Actions pipeline (4 stages)
├── Lakehouse_Architectural_Diagram.png
├── requirements.txt
└── README.md
```

---

## S3 Bucket Layout

| Zone | Bucket Name | Purpose |
|------|-------------|---------|
| Raw | `lakehouse-dev-834424012278-raw` | Landing zone for incoming CSV files |
| DWH | `lakehouse-dev-834424012278-dwh` | Delta Lake tables (partitioned by date) |
| Archived | `lakehouse-dev-834424012278-archived` | Post-ingestion raw files, organized by `dataset/YYYY-MM-DD/` |
| Rejected | `lakehouse-dev-834424012278-rejected` | Records failing validation, with `rejection_reason` column |
| Scripts | `lakehouse-dev-834424012278-glue-scripts` | Glue ETL scripts + Lambda zip (managed by CI/CD) |
| Athena Results | `lakehouse-dev-834424012278-athena-results` | Athena query output |

> Bucket names include the AWS account ID to guarantee global uniqueness.

---

## Data Model

### Source Data (April 2025)

| Dataset | File | Rows |
|---------|------|------|
| Products | `products.csv` | 1,000 |
| Orders | `orders_apr_2025.csv` | 500 |
| Order Items | `order_items_apr_2025.csv` | 2,768 |

### Delta Lake Tables

| Table | Type | Merge Key | Partition |
|-------|------|-----------|-----------|
| `dim_products` | Dimension (SCD Type 1) | `product_id` | None |
| `fact_orders` | Fact | `order_id` | `date` |
| `fact_order_items` | Fact | `id` | `date` |

All tables use Delta Lake for ACID transactions, idempotent MERGE/upsert, and time-travel queries.

---

## ETL Pipeline

Each of the three Glue jobs follows the same pattern:

```
S3 Raw (CSV)
   │
   ▼  [1] READ — Spark reads CSV with schema enforcement
   │
   ▼  [2] VALIDATE — 5-rule validation engine
   │     ├── validate_not_null()              → reject null PKs
   │     ├── validate_timestamp_format()      → reject unparseable timestamps
   │     ├── validate_positive_amount()       → reject zero/negative amounts
   │     └── validate_referential_integrity() → reject orphan order_items
   │
   ▼  [3] SPLIT — valid records proceed; rejected go to S3 rejected zone
   │
   ▼  [4] DEDUPLICATE — keep latest record per merge key
   │
   ▼  [5] REJECTION RATIO GUARD — abort if > 10% of rows rejected
   │
   ▼  [6] DELTA MERGE (UPSERT) — WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT
   │
S3 DWH Zone (Delta table updated, idempotent)
```

### Validation Rules (`etl/validation/rules.py`)

1. **Null PK check** — rejects rows where the merge key is `NULL`
2. **Timestamp format** — rejects rows where `order_timestamp` cannot be cast to timestamp
3. **Positive amount** — rejects orders with `total_amount ≤ 0`
4. **Referential integrity** — rejects `order_items` with `order_id` or `product_id` not present in their parent tables
5. **Deduplication** — window function keeps the most recent row per merge key

Rejected records are written to the S3 rejected zone with a `rejection_reason` column, organized by rule and date, and queryable via Athena.

---

## Pipeline Orchestration

### Step Functions State Machine

```
DetectNewFiles (Pass)
       │
RunGlueJobsParallel (Parallel)
  ├── Products ETL   (retry 2×, backoff 2×, timeout 10 min)
  ├── Orders ETL     (retry 2×, backoff 2×, timeout 10 min)
  └── OrderItems ETL (retry 2×, backoff 2×, timeout 15 min)
       │ all succeeded
ArchiveRawFiles (Lambda invoke, retry 3×)
       │
RunGlueCrawler (updates Glue Data Catalog, non-blocking on failure)
       │
ValidateWithAthena (COUNT(*) smoke test on all 3 tables, non-blocking)
       │
PipelineSucceeded

On any Glue failure → HandlePipelineFailure → SNS alert → PipelineFailed
```

### EventBridge Trigger

A file upload to any of the three raw prefixes (`products/`, `orders/`, `order_items/`) automatically triggers the state machine via an EventBridge rule — no polling, no manual execution needed.

---

## Infrastructure as Code

All 47 AWS resources are declared in Terraform, version-controlled, and deployed via CI/CD.

**Remote state:**
- State file: `s3://lakehouse-terraform-state-834424012278/lakehouse/terraform.tfstate`
- Lock table: `DynamoDB: lakehouse-terraform-locks`

**Terraform modules:**

| Module | Resources |
|--------|-----------|
| `s3` | 6 S3 buckets |
| `iam` | Glue role, Step Functions role, Lambda role, EventBridge role |
| `glue` | 3 Glue jobs, 1 crawler, Glue database |
| `step_functions` | State machine (ASL injected via `templatefile`) |
| `eventbridge` | EventBridge rule + Step Functions target |
| `athena` | Athena workgroup |

---

## CI/CD Pipeline

GitHub Actions triggers on every push to `main`, running four sequential stages:

```
Stage 1: LINT       flake8 on etl/, config/, tests/
Stage 2: TEST       pytest (28 tests) with PySpark + Delta Lake
Stage 3: TERRAFORM  fmt check → init → validate → apply S3 → seed Lambda → plan → apply all
Stage 4: DEPLOY     zip etl/ + config/ → s3 sync scripts → upload Lambda zip
```

**Authentication:** GitHub Actions assumes an IAM role via OIDC — no long-lived AWS credentials stored as secrets.

**Two-phase Terraform apply:** S3 buckets are created first (`-target=module.s3`), the Lambda deployment zip is uploaded to S3, then the full apply runs. This resolves the chicken-and-egg dependency where the Lambda resource requires its zip to already exist in S3.

---

## Running Tests Locally

```bash
pip install -r requirements.txt
PYTHONPATH=. pytest tests/ -v
```

Requirements: Python 3.11, Java 11 (required by PySpark), and the following environment set up:
- `SPARK_HOME` must be **unset** (conflicts with PySpark 3.5.x on Windows)
- `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` pointed to your Python interpreter

The test suite runs against a local PySpark session (not mocked) and covers:
- Clean data loading into Delta tables
- Null PK rejection
- Duplicate deduplication (keep latest)
- Timestamp and amount validation
- Referential integrity enforcement
- Rejection ratio guard

---

## Deployment Results

All 4,268 records loaded correctly with zero data loss:

| Table | Rows Loaded |
|-------|-------------|
| `dim_products` | 1,000 |
| `fact_orders` | 500 |
| `fact_order_items` | 2,768 |

Verified via Athena `COUNT(*)` query at the end of the Step Functions execution.
