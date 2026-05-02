# syntax=docker/dockerfile:1

FROM python:3.10.14-slim

WORKDIR /app

ENV CONFIG_PATH="/app/config.yaml" \
    AIRFLOW_HOME="/app" \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////tmp/airflow_test.db" \
    AIRFLOW__CORE__DAGS_FOLDER="/app/dags" \
    AIRFLOW__CORE__LOAD_EXAMPLES="False" \
    AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry

RUN poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* /app/

RUN --mount=type=cache,target=/root/.cache/pypoetry \
    poetry install --no-interaction --no-ansi --no-root

COPY config.template /app/config.template
RUN cat config.template > config.yaml

RUN echo '\
import os\n\
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow_test.db"\n\
os.environ["AIRFLOW_HOME"] = "/app/airflow_home"\n\
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"\n\
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "ERROR"\n\
' > conftest.py

RUN airflow db init

COPY . /app

ENTRYPOINT ["poetry", "run", "pytest", "-v"]