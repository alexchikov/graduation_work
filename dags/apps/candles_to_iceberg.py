from __future__ import annotations

import argparse
import os

from dags.apps.common_processing import AppConfig, build_spark, load_raw_df, write_iceberg

APP_CONFIG = AppConfig(raw_prefix="raw/moex/candles", payload_block="candles", table_name="processed.moex_fact_security_candles")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--bucket", default=os.getenv("AWS_BUCKET"))
    parser.add_argument("--warehouse", default=os.getenv("ICEBERG_WAREHOUSE"))
    args = parser.parse_args()

    if not args.bucket:
        raise ValueError("AWS_BUCKET is required")
    if not args.warehouse:
        raise ValueError("ICEBERG_WAREHOUSE is required")

    cfg = APP_CONFIG
    spark = build_spark(app_name=cfg.table_name, warehouse=args.warehouse)
    df = load_raw_df(spark=spark, cfg=cfg, bucket=args.bucket, run_date=args.run_date)
    write_iceberg(df=df, table_name=cfg.table_name)
    spark.stop()


if __name__ == "__main__":
    main()
