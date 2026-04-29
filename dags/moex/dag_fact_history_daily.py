from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from dags.moex.common import http_json, put_json_to_s3

URLS = {
    "shares": (
        "https://iss.moex.com/iss/history/engines/stock/markets/"
        "shares/securities.json"
    ),
    "bonds": (
        "https://iss.moex.com/iss/history/engines/stock/markets/"
        "bonds/securities.json"
    ),
}


@dag(
    dag_id="moex_fact_history_daily",
    schedule="0 19 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["moex", "fact", "history"],
)
def moex_fact_history_daily() -> None:
    @task
    def load_market(market: str, run_date: str) -> str:
        payload = http_json(URLS[market], params={"date": run_date})
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = (
            f"raw/moex/history/{market}/date={run_date}/"
            f"payload_{ts}.json"
        )
        return put_json_to_s3(key, payload)

    load_market.override(task_id="load_history_shares")("shares", "{{ ds }}")
    load_market.override(task_id="load_history_bonds")("bonds", "{{ ds }}")


MOEX_FACT_HISTORY_DAILY_DAG = moex_fact_history_daily()
