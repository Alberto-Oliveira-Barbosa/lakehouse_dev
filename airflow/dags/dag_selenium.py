from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawlers.selenium.web_crawler import get_news_from_site

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 22),
    "retries": 0,
}

with DAG(
    "Web_Crawler",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ingest", "minio", "selenium"],
) as dag:

    get_news_from_site = PythonOperator(
        task_id="get_news_from_site",
        python_callable=get_news_from_site
    )

    get_news_from_site

