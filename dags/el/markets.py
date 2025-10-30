from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
import io
from dags.utils.cfg.configs import Config as cfg
from dags.utils.notifiers.tg import TelegramNotifier
from dags.utils.securities import create_s3

DAG_ID = "el__moex_markets"
START = datetime(2025, 10, 29, 0, 10, 0)
DESCRIPTION = "DAG for loading all markets for all MOEX engines into one file"

DEFAULT_ARGS = {
    "owner": "alexc",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

ENGINES_S3_KEY = "raw/engines/moex__engines.json"
MARKETS_DEST_KEY = "raw/markets/moex__all_markets.json"
MARKETS_BASE_URL = cfg.get("URL_MARKETS")


def load_all_markets(**context):
    s3 = create_s3()
    bucket = cfg.get("AWS_BUCKET")

    obj = s3.get_object(Bucket=bucket, Key=ENGINES_S3_KEY)
    engines_data = json.loads(obj["Body"].read().decode("utf-8"))

    engines = engines_data.get("engines", {}).get("data", [])
    columns = engines_data.get("engines", {}).get("columns", [])
    name_idx = columns.index("name")

    all_markets = {}

    for eng in engines:
        engine_name = eng[name_idx]
        url = MARKETS_BASE_URL.format(engine=engine_name)
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        all_markets[engine_name] = data.get("markets", {})  # собираем markets по engine

    json_bytes = io.BytesIO(json.dumps(all_markets, ensure_ascii=False, indent=2).encode("utf-8"))
    s3.upload_fileobj(Fileobj=json_bytes, Bucket=bucket, Key=MARKETS_DEST_KEY)
    return f"All markets uploaded to s3://{bucket}/{MARKETS_DEST_KEY}"


with DAG(
        dag_id=DAG_ID,
        start_date=START,
        description=DESCRIPTION,
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        catchup=False,
        tags=["el", "moex", "markets"],
        on_failure_callback=TelegramNotifier(
            message="DAG el__moex_markets failed!",
            bot_token=cfg.get("TOKEN"),
            chat_id=cfg.get("CHAT_ID")
        ),
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command="echo start markets DAG"
    )

    load_task = PythonOperator(
        task_id="load_all_markets",
        python_callable=load_all_markets,
        provide_context=True
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo end markets DAG"
    )

    start >> load_task >> end
