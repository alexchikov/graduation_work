from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from dags.utils.moex.common import cfg, http_json, put_json_to_s3

URL_TMPL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
    "TQBR/securities/{secid}/candles.json"
)


@dag(
    dag_id="moex_fact_candles_daily",
    schedule="30 19 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["moex", "fact", "candles"],
)
def moex_fact_candles_daily() -> None:
    @task
    def load_for_secid(secid: str, run_date: str) -> str:
        days = int(cfg("MOEX_CANDLES_LOOKBACK_DAYS", "30"))
        date_to = datetime.strptime(run_date, "%Y-%m-%d")
        date_from = (date_to - timedelta(days=days)).strftime("%Y-%m-%d")

        payload = http_json(
            URL_TMPL.format(secid=secid),
            params={"from": date_from, "till": run_date, "interval": 24},
        )
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = (
            f"raw/moex/candles/secid={secid}/date={run_date}/"
            f"payload_{ts}.json"
        )
        return put_json_to_s3(key, payload)

    secids_raw = cfg("MOEX_CANDLES_SECIDS", "SBER,GAZP,LKOH")
    secids = [item.strip() for item in secids_raw.split(",") if item.strip()]
    for secid in secids:
        load_for_secid.override(task_id=f"load_candles_{secid}")(
            secid,
            "{{ ds }}",
        )


MOEX_FACT_CANDLES_DAILY_DAG = moex_fact_candles_daily()
