# Lakehouse Architecture — E-Commerce Transactions

A production-grade Lakehouse architecture for an e-commerce platform on AWS. The system ingests raw transactional data from Amazon S3, cleans and deduplicates it using Delta Lake, and exposes it for downstream analytics through Amazon Athena.

## Architecture Overview

```
CSV/XLSX Sources → S3 Raw Zone → Step Functions → Glue + PySpark + Delta Lake
                                                          ↓
                            Athena ← Glue Catalog ← S3 Lakehouse-DWH Zone (Delta Tables)
```

## Project Structure

```
lakehouse-project/
├── etl/
│   ├── jobs/
│   │   ├── etl_products.py          # Products dimension ETL
│   │   ├── etl_orders.py            # Orders fact ETL
│   │   └── etl_order_items.py       # Order items fact ETL
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── spark_session.py         # Spark session factory with Delta config
│   │   ├── s3_utils.py              # S3 read/write/archive helpers
│   │   └── logger.py                # Structured logging
│   └── validation/
│       ├── __init__.py
│       └── rules.py                 # Validation rules engine
├── orchestration/
│   └── step_function_definition.json # AWS Step Functions ASL
├── config/
│   └── settings.py                  # Centralized configuration
├── tests/
│   ├── __init__.py
│   ├── test_etl_products.py
│   ├── test_etl_orders.py
│   ├── test_etl_order_items.py
│   └── test_validation_rules.py
├── scripts/
│   └── deploy_glue_jobs.sh          # Deployment helper
├── .github/
│   └── workflows/
│       └── ci_cd.yml                # GitHub Actions pipeline
├── requirements.txt
└── README.md
```

## S3 Bucket Layout

| Zone | Bucket Path | Purpose |
|------|-------------|---------|
| Raw | `s3://lakehouse-raw/{dataset}/` | Landing zone for incoming CSVs/XLSX |
| DWH | `s3://lakehouse-dwh/{table}/` | Delta Lake tables (partitioned by date) |
| Archived | `s3://lakehouse-archived/{dataset}/{date}/` | Post-ingestion raw file archive |
| Rejected | `s3://lakehouse-rejected/{rule}/{date}/` | Records failing validation |

## Delta Lake Tables

| Table | Type | Merge Key | Partition |
|-------|------|-----------|-----------|
| `dim_products` | Dimension (SCD1) | `product_id` | None |
| `fact_orders` | Fact | `order_id` | `date` |
| `fact_order_items` | Fact | `id` | `date` |

## Validation Rules

- No null primary identifiers (`product_id`, `order_id`, `id`)
- Valid ISO timestamps on `order_timestamp`
- Referential integrity: `order_items.order_id → orders.order_id`
- Referential integrity: `order_items.product_id → products.product_id`
- Deduplication on primary keys across ingestion batches

## Running Locally

```bash
pip install pyspark delta-spark pytest
pytest tests/ -v
```

## CI/CD

GitHub Actions triggers on push to `main`:
1. Lint with flake8
2. Unit tests with pytest
3. Deploy Glue scripts to S3
4. Deploy Step Function definition
