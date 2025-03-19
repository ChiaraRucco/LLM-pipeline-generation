import time
from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import requests
import json
import time

import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'


def fetch_data(**kwargs):
    start_time = time.time()
    print("Fetching data from API...")
    response = requests.get('https://api.tfl.gov.uk/Place/Meta/Categories')
    data = response.json()
    print("Data fetched:", data)
    kwargs['ti'].xcom_push(key='fetched_data', value=data)
    end_time = time.time()
    print(f"Fetch data execution time: {end_time - start_time} seconds")

def transform_data(**kwargs):
    start_time = time.time()
    fetched_data = kwargs['ti'].xcom_pull(key='fetched_data', task_ids='fetch_data')
    print("Transforming data...")
    transformed_data = [{"id": item["id"], "name": item["name"]} for item in fetched_data]
    print("Transformed data:", transformed_data)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    end_time = time.time()
    print(f"Transform data execution time: {end_time - start_time} seconds")

def post_data(**kwargs):
    start_time = time.time()
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    print("Posting data to API...")
    response = requests.post('https://httpbin.org/post', json=transformed_data)
    print("Post response:", response.json())
    end_time = time.time()
    print(f"Post data execution time: {end_time - start_time} seconds")

default_args = {
    'owner': 'Chiara',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'gen1_claude',
    default_args=default_args,
    description='An ETL pipeline that fetches data from an API, transforms it, and posts it to another API',
    schedule_interval=None,
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

post_data_task = PythonOperator(
    task_id='post_data',
    python_callable=post_data,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> transform_data_task >> post_data_task

start_time = time.time()
print("ETL pipeline execution started.")
end_time = time.time()
print(f"Total execution time: {end_time - start_time} seconds")