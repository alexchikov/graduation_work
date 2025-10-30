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

DAG_ID = "el__moex_engines"
START = datetime(2025, 10, 29, 0, 0, 0)
DESCRIPTION = "DAG for loading list of available MOEX engines"

DEFAULT_ARGS = {
    "owner": "alexc",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

URL = cfg.get("URL_ENGINES")
DEST_PATH = "raw/engines/moex__engines.json"


def load_engines_to_s3(**context):
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()

    s3 = create_s3()
    json_bytes = io.BytesIO(json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
    s3.upload_fileobj(
        Fileobj=json_bytes,
        Bucket=cfg.get("AWS_BUCKET"),
        Key=DEST_PATH
    )

    return f"Uploaded to s3://{cfg.get('AWS_BUCKET')}/{DEST_PATH}"


with DAG(
        dag_id=DAG_ID,
        start_date=START,
        description=DESCRIPTION,
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        catchup=False,
        tags=["el", "moex", "engines"],
        on_failure_callback=TelegramNotifier(
            message="DAG el__moex_engines failed!",
            bot_token=cfg.get("TOKEN"),
            chat_id=cfg.get("CHAT_ID")
        ),
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command="echo start engines DAG"
    )

    load_task = PythonOperator(
        task_id="load_engines_to_s3",
        python_callable=load_engines_to_s3,
        provide_context=True
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo end engines DAG"
    )

    start >> load_task >> end
