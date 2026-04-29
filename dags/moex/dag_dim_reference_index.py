from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from dags.moex.common import http_json, put_json_to_s3


@dag(
    dag_id="moex_dim_reference_index",
    schedule="15 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["moex", "dim", "index"],
)
def moex_dim_reference_index() -> None:
    @task
    def load(logical_date: str) -> str:
        payload = http_json("https://iss.moex.com/iss/index.json")
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = f"raw/moex/index/dt={logical_date}/payload_{ts}.json"
        return put_json_to_s3(key, payload)

    load("{{ ds }}")


moex_dim_reference_index()
