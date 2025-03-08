import time
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago




import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'


default_args = {
    'owner': 'Chiara',
    'start_date': days_ago(1),
}

def extract_data(**kwargs):
    print("Starting extraction...")
    start_time = time.time()
    response = requests.get('https://api.tfl.gov.uk/Place/Meta/Categories')
    response.raise_for_status()
    data = response.json()
    print(f"Extracted {len(data)} records")
    return {'data': data, 'start_time': start_time}

def transform_data(**kwargs):
    ti = kwargs['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    data = extract_result['data']
    print("Transforming data...")
    
    # Basic transformation: Extract category names and count
    categories = [item['category'] for item in data]
    transformed_data = {
        'categories': categories,
        'count': len(categories)
    }
    
    print(f"Transformed data with {transformed_data['count']} categories")
    return {'transformed_data': transformed_data, 'start_time': extract_result['start_time']}

def load_data(**kwargs):
    ti = kwargs['ti']
    transform_result = ti.xcom_pull(task_ids='transform')
    transformed_data = transform_result['transformed_data']
    start_time = transform_result['start_time']
    
    print("Loading data...")
    response = requests.post('https://httpbin.org/post', json=transformed_data)
    response.raise_for_status()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Load response status: {response.status_code}")
    print(f"Total pipeline execution time: {elapsed_time:.2f} seconds")

with DAG(
    'gen1_qwen',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True
    )

    extract >> transform >> load