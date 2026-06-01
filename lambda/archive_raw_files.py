"""
Lambda: Archive Raw Files

Moves processed raw files from the landing zone to the archive bucket,
organized by dataset and ingestion date. Called by Step Functions after
all three Glue jobs succeed.

Event payload:
    {
        "source_bucket": "lakehouse-dev-...-raw",
        "archive_bucket": "lakehouse-dev-...-archived",
        "datasets": ["products", "orders", "order_items"]
    }
"""
import boto3
from datetime import datetime, timezone


def handler(event, context):
    s3 = boto3.client("s3")
    source_bucket = event["source_bucket"]
    archive_bucket = event["archive_bucket"]
    datasets = event.get("datasets", ["products", "orders", "order_items"])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    archived = []
    paginator = s3.get_paginator("list_objects_v2")

    for dataset in datasets:
        prefix = f"{dataset}/"
        for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                filename = src_key.split("/")[-1]
                if not filename:
                    continue

                dest_key = f"{dataset}/{today}/{filename}"

                s3.copy_object(
                    CopySource={"Bucket": source_bucket, "Key": src_key},
                    Bucket=archive_bucket,
                    Key=dest_key,
                )
                s3.delete_object(Bucket=source_bucket, Key=src_key)

                archived.append({"src": src_key, "dest": dest_key})
                print(f"Archived: s3://{source_bucket}/{src_key} → s3://{archive_bucket}/{dest_key}")

    print(f"Total files archived: {len(archived)}")
    return {"archived": archived, "count": len(archived)}
