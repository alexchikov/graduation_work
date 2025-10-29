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

DAG_ID = "el__securities_list"
START = datetime(2025, 10, 28, 0, 0, 0)
DESCRIPTION = "DAG for loading full list of traded securities from MOEX with pagination"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

CURRENT_DATE = "{{ execution_date.strftime('%Y-%m-%d') }}"
BASE_URL = cfg.get("URL_SECURITIES")
DEST_PATH = "raw/securities/moex__securities_list.json"
LIMIT = 100


def load_securities_to_s3(**context):
    all_data = []

    start_index = 0
    while True:
        params = {
            "is_trading": "1",
            "lang": "ru",
            "start": start_index,
            "limit": LIMIT
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        result = response.json()

        securities_data = result.get("securities", {}).get("data", [])
        if not securities_data:
            break

        all_data.extend(securities_data)
        start_index += LIMIT

    securities_json = {
        "securities": {
            "metadata": result["securities"]["metadata"],
            "columns": result["securities"]["columns"],
            "data": all_data
        }
    }

    s3 = create_s3()
    json_bytes = io.BytesIO(json.dumps(securities_json, ensure_ascii=False, indent=2).encode("utf-8"))
    s3.upload_fileobj(
        Fileobj=json_bytes,
        Bucket=cfg.get("AWS_BUCKET"),
        Key=DEST_PATH
    )

    return f"Uploaded {len(all_data)} securities to s3://{cfg.get('AWS_BUCKET')}/{DEST_PATH}"


with DAG(
        dag_id=DAG_ID,
        start_date=START,
        description=DESCRIPTION,
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        catchup=False,
        tags=["el", "securities", "moex"],
        on_failure_callback=TelegramNotifier(
            message="DAG el__securities_list failed!",
            bot_token=cfg.get("TOKEN"),
            chat_id=cfg.get("CHAT_ID")
        ),
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command=f"echo start $(pwd) {CURRENT_DATE}"
    )

    load_task = PythonOperator(
        task_id="load_securities_to_s3",
        python_callable=load_securities_to_s3,
        provide_context=True,
    )

    end = BashOperator(
        task_id="end",
        bash_command=f"echo 'end {CURRENT_DATE}'"
    )

    start >> load_task >> end
