FROM python:3.10.14-slim

WORKDIR /app

COPY . .

RUN printf "TOKEN: \"1111\"\nCHAT_ID: \"1111\"\n" > config.yaml

ENV CONFIG_PATH="/app/config.yaml"
ENV AIRFLOW_HOME="/app"
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////tmp/airflow_test.db"
ENV AIRFLOW__CORE__DAGS_FOLDER="/app/dags"
ENV AIRFLOW__CORE__LOAD_EXAMPLES="False"
ENV AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR"

RUN apt-get update && \
    apt-get install -y python3-virtualenv python3-poetry && \
    rm -rf /var/lib/apt/lists/*

RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi

RUN echo '\
import os\n\
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow_test.db"\n\
os.environ["AIRFLOW_HOME"] = "/app/airflow_home"\n\
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"\n\
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "ERROR"\n\
' > conftest.py

RUN airflow db init

ENTRYPOINT ["poetry", "run", "pytest", "-v"]
