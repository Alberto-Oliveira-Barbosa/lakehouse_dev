from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.bronze.ingest_on_bronze import ingest_test
from pipelines.silver.ingest_on_silver import transform_test
from pipelines.gold.ingest_on_gold import generate_gold_metrics

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 16),
    "retries": 0,
}

with DAG(
    "Pipeline_Bronze_to_Gold",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["bronze", "ingest", "minio", "spark"],
) as dag:

    ingest_test = PythonOperator(
        task_id="ingest_test",
        python_callable=ingest_test
    )

    transform_test = PythonOperator(
        task_id="transform_test",
        python_callable=transform_test
    )

    aggregate_test = PythonOperator(
        task_id="generate_gold_metrics",
        python_callable=generate_gold_metrics
    )

    ingest_test >> transform_test >> aggregate_test 
