from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags.utils.notifiers.tg import TelegramNotifier
from dags.utils.cfg.configs import Config as cfg
from dags.utils.securities import create_s3, load_data


DAG_ID = "el__moex_securities"
START = datetime(2013, 1, 1, 0, 0, 0)
DESCRIPTION = "DAG for ETL processing MOEX securities"

DEFAULT_ARGS = {
    "owner": "alexc",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

CURRENT_DATE = "{{ execution_date.strftime('%Y-%m-%d') }}"
FILENAME = f"moex_security_{CURRENT_DATE.replace('-', '')}.json"
DEST_URL = f"raw/history/securities/{FILENAME}"


def create_s3_task(**context):
    s3 = create_s3()
    context['ti'].xcom_push(key='s3_params', value={
        "bucket": cfg.get("AWS_BUCKET"),
        "key": DEST_URL
    })
    return "S3 client created"


def load_data_task(**context):
    ti = context['ti']
    s3_params = ti.xcom_pull(task_ids='create_s3', key='s3_params')
    if not s3_params:
        raise ValueError("Не удалось получить параметры S3 из XCom")

    s3 = create_s3()
    load_data(
        url=cfg.get('URL_MOEX_SECURITIES'),
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
    catchup=True,
    tags=["moex", "el"],
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
