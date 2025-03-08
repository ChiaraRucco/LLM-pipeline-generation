import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'


# Define the API URL
API_URL_READ = "https://jsonplaceholder.typicode.com/posts"  # API to read from

# ETL Function for GET request
def extract():
    print("Starting the extraction process...")
    try:
        response = requests.get(API_URL_READ, timeout=10)  # Set a timeout of 10 seconds
        response.raise_for_status()  # Raise an exception for 4xx/5xx errors
        print("Data extracted successfully!")
        return response.json()  # Return the data (assuming it's JSON)
    except requests.exceptions.Timeout:
        print("Request timed out.")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise

# Define the default_args
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'owner': 'Chiara',  # Set owner as Chiara
}

# Define the DAG
dag = DAG(
    'api_get_request_with_timeout',
    description='DAG with a GET request and a timeout',
    schedule_interval=None,
    start_date=datetime(2025, 3, 7),
    catchup=False,
    default_args=default_args  # Apply default_args to the DAG
)

# Define the task using PythonOperator
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

# Set the task sequence
extract_task

