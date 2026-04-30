from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession


def build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("moex_data_quality_check")
        .config("spark.sql.catalog.processed", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.processed.type", "hadoop")
        .config("spark.sql.catalog.processed.warehouse", warehouse)
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--warehouse", default=os.getenv("ICEBERG_WAREHOUSE"))
    args = parser.parse_args()

    if not args.warehouse:
        raise ValueError("ICEBERG_WAREHOUSE is required")

    spark = build_spark(args.warehouse)
    df = spark.table(args.table)

    total = df.count()
    if total == 0:
        raise ValueError(f"Data quality failed: table {args.table} is empty")

    null_rows = df.filter("run_date is null").count() if "run_date" in df.columns else 0
    if null_rows > 0:
        raise ValueError(
            f"Data quality failed: table {args.table} has {null_rows} rows with null run_date"
        )

    spark.stop()


if __name__ == "__main__":
    main()
