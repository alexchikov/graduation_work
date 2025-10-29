from airflow.models import DagBag
from unittest.mock import patch
import pytest


SOURCES = ["imoex", "rtsi", "mmix", "agro", "inav"]

@pytest.fixture(autouse=True)
def mock_telegram_bot():
    with patch("dags.utils.notifiers.tg.Bot") as mock_bot:
        mock_bot.return_value = None
        yield

@pytest.fixture(autouse=True)
def dagbag():
    return DagBag()

def test_dag_loaded(dagbag):
    for src in SOURCES:
        dag = dagbag.get_dag(dag_id=f"el__{src}_securities")
        assert dagbag.import_errors == {}

def test_dag_is_not_none(dagbag):
    for src in SOURCES:
        dag = dagbag.get_dag(dag_id=f"el__{src}_securities")
        assert dag is not None
