from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags.utils.notifiers.tg import TelegramNotifier
from dags.utils.cfg.configs import Config as cfg
from dags.utils.securities import create_s3, load_data

DAG_ID = "el__mmix_securities"
START = datetime(2025, 10, 28, 0, 0, 0)
DESCRIPTION = "DAG for ETL processing MMIX securities"

DEFAULT_ARGS = {
    "owner": "alexc",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

CURRENT_DATE = "{{ execution_date.strftime('%Y-%m-%d') }}"
SOURCE = "mmix"
s3 = create_s3()


def create_s3_task(**context):
    filename = f"{SOURCE}__securities.json"
    dest_url = f"raw/history/securities/{SOURCE}/{filename}"
    context['ti'].xcom_push(key='s3_params', value={
        "bucket": cfg.get("AWS_BUCKET"),
        "key": dest_url
    })
    return "S3 client created"


def load_data_task(**context):
    ti = context['ti']
    s3_params = ti.xcom_pull(task_ids='create_s3', key='s3_params')
    if not s3_params:
        raise ValueError("Cannot get S3 parameters from XCom")

    s3 = create_s3()
    load_data(
        url=cfg.get('URL_SECURITIES') + f"{SOURCE.upper()}.json",
        bucket=s3_params["bucket"],
        key=s3_params["key"],
        s3=s3
    )


with DAG(
        dag_id=DAG_ID,
        start_date=START,
        description=DESCRIPTION,
        default_args=DEFAULT_ARGS,
        schedule="@daily",
        catchup=False,
        tags=[SOURCE, "el", "securities"],
        on_failure_callback=TelegramNotifier(
            message='dag failed!',
            bot_token=cfg.get("TOKEN"),
            chat_id=cfg.get("CHAT_ID")
        ),
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command=f"echo start $(pwd) {CURRENT_DATE}"
    )

    create_s3_op = PythonOperator(
        task_id='create_s3',
        python_callable=create_s3_task,
        provide_context=True
    )

    load_data_op = PythonOperator(
        task_id='load_data',
        python_callable=load_data_task,
        provide_context=True
    )

    end = BashOperator(
        task_id='end',
        bash_command=f"echo 'end {CURRENT_DATE}'"
    )

    start >> create_s3_op >> load_data_op >> end
