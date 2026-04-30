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


def _build_s3_client():
    import boto3

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")

    client_kwargs = {"endpoint_url": "https://storage.yandexcloud.net"}
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs.update(
            {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            }
        )
    if aws_session_token:
        client_kwargs["aws_session_token"] = aws_session_token

    return boto3.client("s3", **client_kwargs)


def list_objects(bucket: str, prefix: str, run_date: str) -> list[str]:
    s3 = _build_s3_client()
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

    s3 = _build_s3_client()

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
