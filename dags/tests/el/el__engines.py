from airflow.models import DagBag
from unittest.mock import patch
import pytest

DAG_ID = "el__moex_engines"


@pytest.fixture(autouse=True)
def mock_telegram_bot():
    with patch("dags.utils.notifiers.tg.Bot") as mock_bot:
        mock_bot.return_value = None
        yield


@pytest.fixture(autouse=True)
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    assert dagbag.import_errors == {}


def test_dag_is_not_none(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    assert dag is not None


def test_dag(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    assert dag.test()
