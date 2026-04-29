from dags.moex.dag_bridge_tqbr_securities import (
    MOEX_BRIDGE_TQBR_SECURITIES_DAG,
)
from dags.moex.dag_dim_reference_index import MOEX_DIM_REFERENCE_INDEX_DAG
from dags.moex.dag_dim_security_full import MOEX_DIM_SECURITY_FULL_DAG
from dags.moex.dag_fact_candles_daily import MOEX_FACT_CANDLES_DAILY_DAG
from dags.moex.dag_fact_history_daily import MOEX_FACT_HISTORY_DAILY_DAG
from dags.moex import common


def test_dim_reference_index_dag():
    assert MOEX_DIM_REFERENCE_INDEX_DAG.dag_id == "moex_dim_reference_index"
    assert "load" in MOEX_DIM_REFERENCE_INDEX_DAG.task_ids


def test_dim_security_full_dag():
    assert MOEX_DIM_SECURITY_FULL_DAG.dag_id == "moex_dim_security_full"
    assert "load" in MOEX_DIM_SECURITY_FULL_DAG.task_ids


def test_bridge_tqbr_securities_dag():
    assert (
        MOEX_BRIDGE_TQBR_SECURITIES_DAG.dag_id
        == "moex_bridge_tqbr_securities"
    )
    assert "load" in MOEX_BRIDGE_TQBR_SECURITIES_DAG.task_ids


def test_fact_history_daily_dag():
    assert MOEX_FACT_HISTORY_DAILY_DAG.dag_id == "moex_fact_history_daily"
    assert "load_history_shares" in MOEX_FACT_HISTORY_DAILY_DAG.task_ids
    assert "load_history_bonds" in MOEX_FACT_HISTORY_DAILY_DAG.task_ids


def test_fact_candles_daily_dag_has_default_secids():
    assert MOEX_FACT_CANDLES_DAILY_DAG.dag_id == "moex_fact_candles_daily"
    assert "load_candles_SBER" in MOEX_FACT_CANDLES_DAILY_DAG.task_ids
    assert "load_candles_GAZP" in MOEX_FACT_CANDLES_DAILY_DAG.task_ids
    assert "load_candles_LKOH" in MOEX_FACT_CANDLES_DAILY_DAG.task_ids


def test_cfg_reads_local_config_fallback(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY", raising=False)
    monkeypatch.setattr(
        common,
        "_load_local_config",
        lambda: {"AWS_ACCESS_KEY": "x"},
    )
    assert common.cfg("AWS_ACCESS_KEY") == "x"
