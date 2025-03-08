from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import time



import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'
# Define default arguments for the DAG
default_args = {
    'owner': 'Chiara',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'gen1_deepseek',
    default_args=default_args,
    description='ETL pipeline to fetch data from TFL API, transform, and post to another API',
    schedule_interval='@daily',
)

# Function to fetch data from the TFL API
def fetch_data():
    url = "https://api.tfl.gov.uk/Place/Meta/Categories"
    print("Fetching data from TFL API...")
    response = requests.get(url)
    if response.status_code == 200:
        print("Data fetched successfully!")
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

# Function to transform the data
def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    print("Transforming data...")
    
    # Example transformation: Extract only the 'category' field from each item
    transformed_data = [item['category'] for item in data]
    print(f"Transformed data: {transformed_data}")
    return transformed_data

# Function to post data to the target API
def post_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    url = "https://httpbin.org/post"
    headers = {'Content-Type': 'application/json'}
    print("Posting data to target API...")
    
    response = requests.post(url, data=json.dumps(transformed_data), headers=headers)
    if response.status_code == 200:
        print("Data posted successfully!")
        print(f"Response from target API: {response.json()}")
    else:
        raise Exception(f"Failed to post data: {response.status_code}")

# Function to measure and print execution time
def measure_execution_time(**kwargs):
    ti = kwargs['ti']
    start_time = ti.xcom_pull(task_ids='start_time_task')
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")

# Task to record the start time
def record_start_time():
    return time.time()

# Define tasks
start_time_task = PythonOperator(
    task_id='start_time_task',
    python_callable=record_start_time,
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
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

measure_time_task = PythonOperator(
    task_id='measure_execution_time',
    python_callable=measure_execution_time,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
start_time_task >> fetch_data_task >> transform_data_task >> post_data_task >> measure_time_task