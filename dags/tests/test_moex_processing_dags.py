from pathlib import Path


DAG_FILES = [
    "dag_process_index.py",
    "dag_process_securities_full.py",
    "dag_process_tqbr_securities.py",
    "dag_process_history_shares.py",
    "dag_process_history_bonds.py",
    "dag_process_candles.py",
]


def test_processing_dags_have_required_operators_and_steps():
    root = Path("dags/moex_processing")
    for file_name in DAG_FILES:
        content = (root / file_name).read_text(encoding="utf-8")
        assert "S3KeySensor(" in content
        assert "SparkSubmitOperator(" in content
        assert "data_quality_check" in content
        assert "wait_raw >> process >> dq >> done" in content
