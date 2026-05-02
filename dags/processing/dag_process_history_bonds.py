from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="moex_process_history_bonds_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule="0 20 * * *",
    catchup=False,
    tags=["moex", "spark", "iceberg", "processed"],
) as dag:
    wait_raw = S3KeySensor(
        task_id="wait_raw_files",
        bucket_name="{{ var.value.AWS_BUCKET }}",
        bucket_key="raw/moex/history/bonds/date={{ ds }}/*",
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 60 * 12,
    )

    process = SparkSubmitOperator(
        task_id="process_to_iceberg",
        application="/home/airflow/dags/apps/history_bonds_to_iceberg.py",
        application_args=[
            "--run-date", "{{ ds }}",
            "--bucket", "{{ var.value.AWS_BUCKET }}",
            "--warehouse", "{{ var.value.ICEBERG_WAREHOUSE }}",
        ],
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.AWS_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.AWS_SECRET_KEY }}",
            "spark.hadoop.fs.s3a.endpoint": "https://storage.yandexcloud.net/",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
        },
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        env_vars={
            "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY }}",
            "AWS_SECRET_KEY_ID": "{{ var.value.AWS_SECRET_KEY }}",
            "AWS_ENDPOINT_URL": "https://storage.yandexcloud.net/",
        },
    )

    dq = SparkSubmitOperator(
        task_id="data_quality_check",
        application="/home/airflow/dags/apps/data_quality_check.py",
        application_args=[
            "--table", "processed.moex_fact_bond_daily",
            "--warehouse", "{{ var.value.ICEBERG_WAREHOUSE }}",
        ],
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.AWS_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.AWS_SECRET_KEY }}",
            "spark.hadoop.fs.s3a.endpoint": "https://storage.yandexcloud.net/",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
        },
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        env_vars={
            "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY }}",
            "AWS_SECRET_KEY_ID": "{{ var.value.AWS_SECRET_KEY }}",
            "AWS_ENDPOINT_URL": "https://storage.yandexcloud.net/",
        },
    )

    done = EmptyOperator(task_id="done")
    wait_raw >> process >> dq >> done
