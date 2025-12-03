from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.fetch_stock_data import fetch_and_store_stock_data

default_args = {
    "owner": "veena",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "stock_market_data_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
):

    run_etl = PythonOperator(
        task_id="fetch_and_store_stock",
        python_callable=fetch_and_store_stock_data,
    )

    run_etl
