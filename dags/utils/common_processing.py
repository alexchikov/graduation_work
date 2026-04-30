from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class AppConfig:
    raw_prefix: str
    payload_block: str
    table_name: str


def build_spark(app_name: str, warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.processed", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.processed.type", "hadoop")
        .config("spark.sql.catalog.processed.warehouse", warehouse)
        .getOrCreate()
    )


def extract_rows(payload: dict[str, Any], block_name: str) -> list[dict[str, Any]]:
    block = payload.get(block_name, {})
    columns = block.get("columns", [])
    data = block.get("data", [])
    if not columns or not data:
        return []
    return [dict(zip(columns, row)) for row in data]


def list_objects(bucket: str, prefix: str, run_date: str) -> list[str]:
    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        endpoint_url='https://storage.yandexcloud.net',
    )
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if f"dt={run_date}/" in key or f"date={run_date}/" in key:
                keys.append(key)
    return keys


def load_raw_df(spark: SparkSession, cfg: AppConfig, bucket: str, run_date: str) -> DataFrame:
    keys = list_objects(bucket=bucket, prefix=cfg.raw_prefix, run_date=run_date)
    if not keys:
        raise ValueError(f"No raw files found for {cfg.raw_prefix} and date {run_date}")

    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        endpoint_url='https://storage.yandexcloud.net'
    )

    rows: list[dict[str, Any]] = []
    for key in keys:
        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        for row in extract_rows(payload, cfg.payload_block):
            row["source_key"] = key
            row["run_date"] = run_date
            rows.append(row)

    if not rows:
        raise ValueError(f"No rows extracted from {cfg.payload_block}")

    return spark.createDataFrame(rows).dropDuplicates()


def write_iceberg(df: DataFrame, table_name: str) -> None:
    df.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").createOrReplace()
