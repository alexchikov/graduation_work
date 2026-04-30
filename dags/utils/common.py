"""Shared helpers for Airflow DAGs and Spark apps."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import requests
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REQUEST_TIMEOUT_SEC = 30
CONFIG_FILE_NAME = "config.yaml"


_TYPE_MAPPING = {
    "string": StringType(),
    "str": StringType(),
    "text": StringType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "number": DoubleType(),
    "real": DoubleType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "bool": BooleanType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "datetime": TimestampType(),
    "timestamp": TimestampType(),
}


def _spark_type_from_metadata(meta: Any):
    if isinstance(meta, dict):
        type_name = str(meta.get("type") or meta.get("data_type") or "string").lower()
    else:
        type_name = str(meta or "string").lower()
    return _TYPE_MAPPING.get(type_name, StringType())


def _build_schema(payload: dict[str, Any], block_name: str) -> StructType:
    block = payload.get(block_name, {})
    columns = block.get("columns", [])
    metadata = block.get("metadata", {})

    fields: list[StructField] = []
    for column in columns:
        column_meta = metadata.get(column, {}) if isinstance(metadata, dict) else {}
        fields.append(StructField(column, _spark_type_from_metadata(column_meta), True))

    fields.extend([
        StructField("source_key", StringType(), False),
        StructField("run_date", StringType(), False),
    ])
    return StructType(fields)


@dataclass(frozen=True)
class AppConfig:
    raw_prefix: str
    payload_block: str
    table_name: str


def _load_local_config() -> dict[str, str]:
    config_path = Path(__file__).resolve().parents[2] / CONFIG_FILE_NAME
    if not config_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized = value.strip().strip("'").strip('"')
        values[key.strip()] = normalized
    return values


def cfg(name: str, default: str | None = None) -> str | None:
    env_val = os.getenv(name)
    if env_val:
        return env_val
    try:
        value = Variable.get(name)
        if value:
            return value
    except Exception:
        pass
    file_value = _load_local_config().get(name)
    if file_value:
        return file_value
    return default


def bucket() -> str:
    bucket_name = cfg("AWS_BUCKET")
    if not bucket_name:
        raise AirflowFailException("AWS_BUCKET is required")
    return bucket_name


def s3_client() -> Any:
    aws_access_key_id = cfg("AWS_ACCESS_KEY_ID") or cfg("AWS_ACCESS_KEY")
    aws_secret_access_key = cfg("AWS_SECRET_ACCESS_KEY") or cfg("AWS_SECRET_KEY")
    aws_session_token = cfg("AWS_SESSION_TOKEN")

    client_kwargs: dict[str, Any] = {"endpoint_url": "https://storage.yandexcloud.net"}
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


def http_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise AirflowFailException(f"MOEX endpoint request failed: {url}") from exc


def put_json_to_s3(key: str, payload: dict[str, Any]) -> str:
    s3 = s3_client()
    s3.put_object(
        Bucket=bucket(),
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def notify_telegram(message: str) -> None:
    token = cfg("TOKEN")
    chat_id = cfg("CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
    except requests.RequestException:
        return


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


def list_objects(bucket_name: str, prefix: str, run_date: str) -> list[str]:
    s3 = s3_client()
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if f"dt={run_date}/" in key or f"date={run_date}/" in key:
                keys.append(key)
    return keys


def load_raw_df(spark: SparkSession, app_cfg: AppConfig, bucket_name: str, run_date: str) -> DataFrame:
    keys = list_objects(bucket_name=bucket_name, prefix=app_cfg.raw_prefix, run_date=run_date)
    if not keys:
        raise ValueError(f"No raw files found for {app_cfg.raw_prefix} and date {run_date}")

    s3 = s3_client()
    rows: list[dict[str, Any]] = []
    schema: StructType | None = None
    for key in keys:
        payload = json.loads(s3.get_object(Bucket=bucket_name, Key=key)["Body"].read())
        if schema is None:
            schema = _build_schema(payload=payload, block_name=app_cfg.payload_block)
        for row in extract_rows(payload, app_cfg.payload_block):
            row["source_key"] = key
            row["run_date"] = run_date
            rows.append(row)

    if not rows:
        raise ValueError(f"No rows extracted from {app_cfg.payload_block}")
    if schema is None:
        raise ValueError(f"Unable to build schema for {app_cfg.payload_block}")

    return spark.createDataFrame(rows, schema=schema).dropDuplicates()


def write_iceberg(df: DataFrame, table_name: str) -> None:
    df.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").createOrReplace()
