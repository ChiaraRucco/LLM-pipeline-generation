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


def extract_data():
    url = "https://api.tfl.gov.uk/Place/Meta/Categories"
    print(f"Extracting data from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print("Extracted data successfully.")
        return data
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return []

def transform_data(data):
    print("Transforming data...")
    # Basic transformation: filtering categories that contain the word "Transport"
    transformed = [item for item in data if "camera" in item.get("category", "")]
    print(f"Transformed {len(transformed)} records.")
    return transformed

def load_data(transformed_data):
    url = "https://httpbin.org/post"
    print(f"Loading data to {url}")
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(transformed_data), headers=headers)
    if response.status_code == 200:
        print("Data successfully posted.")
    else:
        print(f"Failed to post data. Status Code: {response.status_code}")

def etl_pipeline():
    start_time = time.time()
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    execution_time = time.time() - start_time
    print(f"ETL Pipeline execution time: {execution_time:.2f} seconds")

# Define Airflow DAG
with DAG(
    dag_id="gen1_chat",
    schedule_interval=None,
    catchup=False
) as dag:
    
    etl_task = PythonOperator(
        task_id="etl_task",
        python_callable=etl_pipeline
    )
    
    etl_task
