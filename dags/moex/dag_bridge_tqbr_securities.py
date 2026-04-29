from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from dags.moex.common import http_json, put_json_to_s3

URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
    "TQBR/securities.json"
)


@dag(
    dag_id="moex_bridge_tqbr_securities",
    schedule="25 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["moex", "bridge", "tqbr"],
)
def moex_bridge_tqbr_securities() -> None:
    @task
    def load(run_date: str) -> str:
        payload = http_json(URL)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = f"raw/moex/tqbr_securities/dt={run_date}/payload_{ts}.json"
        return put_json_to_s3(key, payload)

    load("{{ ds }}")


MOEX_BRIDGE_TQBR_SECURITIES_DAG = moex_bridge_tqbr_securities()
