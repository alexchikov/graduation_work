from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

TABLE_MAPPINGS = [
    ("processed.moex_dim_market", "dim_market"),
    ("processed.moex_dim_security", "dim_security"),
    ("processed.moex_bridge_security_board", "bridge_security_board"),
    ("processed.moex_fact_stock_daily", "fact_stock_daily"),
    ("processed.moex_fact_bond_daily", "fact_bond_daily"),
    ("processed.moex_fact_security_candles", "fact_security_candles"),
]

with DAG(
    dag_id="moex_process_iceberg_to_clickhouse",
    start_date=datetime(2025, 1, 1),
    schedule="40 6 * * *",
    catchup=False,
    tags=["moex", "spark", "iceberg", "clickhouse", "gold"],
) as dag:
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    prev = start
    for source_table, target_table in TABLE_MAPPINGS:
        transfer = SparkSubmitOperator(
            task_id=f"load_{target_table}_to_clickhouse",
            application="/home/airflow/dags/apps/iceberg_to_clickhouse.py",
            application_args=[
                "--source-table", source_table,
                "--target-table", target_table,
                "--warehouse", "{{ var.value.ICEBERG_WAREHOUSE }}",
                "--clickhouse-host", "{{ var.value.CLICKHOUSE_HOST }}",
                "--clickhouse-port", "{{ var.value.CLICKHOUSE_PORT }}",
                "--clickhouse-database", "{{ var.value.CLICKHOUSE_DATABASE }}",
                "--clickhouse-user", "{{ var.value.CLICKHOUSE_USER }}",
                "--clickhouse-password", "{{ var.value.CLICKHOUSE_PASSWORD }}",
                "--mode", "overwrite",
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
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "com.clickhouse:clickhouse-jdbc:0.7.1"
            ),
            env_vars={
                "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY }}",
                "AWS_SECRET_KEY_ID": "{{ var.value.AWS_SECRET_KEY }}",
                "AWS_ENDPOINT_URL": "https://storage.yandexcloud.net/",
            },
        )
        prev >> transfer
        prev = transfer

    prev >> done
