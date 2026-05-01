from __future__ import annotations

import argparse
import os

from dags.utils.common import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default=os.getenv("ICEBERG_WAREHOUSE"))
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--clickhouse-host", default=os.getenv("CLICKHOUSE_HOST"))
    parser.add_argument("--clickhouse-port", default=os.getenv("CLICKHOUSE_PORT", "8123"))
    parser.add_argument("--clickhouse-database", default=os.getenv("CLICKHOUSE_DATABASE", "default"))
    parser.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument("--mode", default="append", choices=["overwrite", "append"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.warehouse:
        raise ValueError("ICEBERG_WAREHOUSE is required")
    if not args.clickhouse_host:
        raise ValueError("CLICKHOUSE_HOST is required")

    spark = build_spark(app_name=f"{args.source_table}_to_clickhouse", warehouse=args.warehouse)
    source_df = spark.table(args.source_table)

    (
        source_df.write.mode(args.mode)
        .format("jdbc")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option(
            "url",
            f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/{args.clickhouse_database}",
        )
        .option("dbtable", args.target_table)
        .option("user", args.clickhouse_user)
        .option("password", args.clickhouse_password)
        .save()
    )
    spark.stop()


if __name__ == "__main__":
    main()
