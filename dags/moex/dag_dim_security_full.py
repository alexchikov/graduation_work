from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from dags.moex.common import http_json, put_json_to_s3


@dag(
    dag_id="moex_dim_security_full",
    schedule="20 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["moex", "dim", "security"],
)
def moex_dim_security_full() -> None:
    @task
    def load(run_date: str) -> str:
        payload = http_json("https://iss.moex.com/iss/securities.json")
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = f"raw/moex/securities_full/dt={run_date}/payload_{ts}.json"
        return put_json_to_s3(key, payload)

    load("{{ ds }}")


MOEX_DIM_SECURITY_FULL_DAG = moex_dim_security_full()
