from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.bronze.ingest_on_bronze import ingest_test

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 16),
    "retries": 0,
}

with DAG(
    "ingest_on_bronze_layer",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["bronze", "ingest", "minio", "spark"],
) as dag:

    ingest_test = PythonOperator(
        task_id="ingest_test",
        python_callable=ingest_test
    )

    ingest_test
