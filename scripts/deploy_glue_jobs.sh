#!/usr/bin/env bash
# deploy_glue_jobs.sh

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_GLUE_SCRIPTS_BUCKET="${S3_GLUE_SCRIPTS_BUCKET:-lakehouse-glue-scripts}"
STATE_MACHINE_ARN="${STATE_MACHINE_ARN:-}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================="
echo "  Lakehouse ETL — Manual Deploy"
echo "========================================="
echo "  Region:           ${AWS_REGION}"
echo "  Scripts Bucket:   ${S3_GLUE_SCRIPTS_BUCKET}"
echo "  Project Root:     ${PROJECT_ROOT}"
echo "========================================="

# ── Step 1: Upload ETL scripts to S3 ──────────────────────────────────
echo ""
echo "[1/3] Uploading ETL scripts to S3..."

aws s3 sync "${PROJECT_ROOT}/etl/" \
    "s3://${S3_GLUE_SCRIPTS_BUCKET}/etl/" \
    --delete \
    --exclude "__pycache__/*" \
    --exclude "*.pyc" \
    --region "${AWS_REGION}"

aws s3 cp "${PROJECT_ROOT}/config/settings.py" \
    "s3://${S3_GLUE_SCRIPTS_BUCKET}/config/settings.py" \
    --region "${AWS_REGION}"

echo "  ETL scripts uploaded."

# ── Step 2: Upload Step Function definition ────────────────────────────
echo ""
echo "[2/3] Uploading Step Function definition..."

aws s3 cp "${PROJECT_ROOT}/orchestration/step_function_definition.json" \
    "s3://${S3_GLUE_SCRIPTS_BUCKET}/orchestration/step_function_definition.json" \
    --region "${AWS_REGION}"

echo "  Step Function definition uploaded to S3."

# ── Step 3: Update the live Step Function (if ARN is provided) ─────────
if [ -n "${STATE_MACHINE_ARN}" ]; then
    echo ""
    echo "[3/3] Updating live Step Function: ${STATE_MACHINE_ARN}"

    aws stepfunctions update-state-machine \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --definition "file://${PROJECT_ROOT}/orchestration/step_function_definition.json" \
        --region "${AWS_REGION}"

    echo "  Step Function updated."
else
    echo ""
    echo "[3/3] Skipping Step Function update (STATE_MACHINE_ARN not set)."
    echo "  Set STATE_MACHINE_ARN to update the live state machine."
fi

echo ""
echo "Deploy complete."
