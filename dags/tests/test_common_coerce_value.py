from datetime import date, datetime

from pyspark.sql.types import DateType, TimestampType

from dags.utils.common import _coerce_value


def test_coerce_value_invalid_zero_date_to_none() -> None:
    assert _coerce_value("0000-00-00", DateType()) is None


def test_coerce_value_invalid_zero_timestamp_to_none() -> None:
    assert _coerce_value("0000-00-00 00:00:00", TimestampType()) is None


def test_coerce_value_valid_date_and_timestamp() -> None:
    assert _coerce_value("2026-04-30", DateType()) == date(2026, 4, 30)
    assert _coerce_value("2026-04-30T22:00:26", TimestampType()) == datetime(2026, 4, 30, 22, 0, 26)
