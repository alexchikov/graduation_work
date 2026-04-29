"""Common helpers for MOEX Airflow DAGs."""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
import requests
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

REQUEST_TIMEOUT_SEC = 30


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
    return default


def bucket() -> str:
    bucket_name = cfg("AWS_BUCKET")
    if not bucket_name:
        raise AirflowFailException("AWS_BUCKET is required")
    return bucket_name


def s3_client() -> Any:
    access_key = cfg("AWS_ACCESS_KEY")
    secret_key = cfg("AWS_SECRET_KEY")
    region = cfg("AWS_REGION", "eu-central-1")
    if not access_key or not secret_key:
        raise AirflowFailException("AWS credentials are required")
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
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
