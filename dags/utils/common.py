"""Common helpers for MOEX Airflow DAGs."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import boto3
import requests
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

REQUEST_TIMEOUT_SEC = 30
CONFIG_FILE_NAME = "config.yaml"


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
    access_key = cfg("AWS_ACCESS_KEY")
    secret_key = cfg("AWS_SECRET_KEY")
    if not access_key or not secret_key:
        raise AirflowFailException("AWS credentials are required")
    return boto3.client(
        "s3",
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def http_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        response = requests.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        msg = f"MOEX endpoint request failed: {url}"
        raise AirflowFailException(msg) from exc


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
        payload = {"chat_id": chat_id, "text": message}
        requests.post(url, json=payload, timeout=10)
    except requests.RequestException:
        return
