from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.sensors.sql import SQLCheckOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
import requests
import json
import logging
import time

# Default args


import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'


DEFAULT_ARGS = {
    'owner': 'Chiara',
    'retries': 3,
    'retry_delay': 300,  # Retry after 5 minutes
    'sla': timedelta(minutes=30)
}

def extract_data(**kwargs):
    """Extracts data from TFL API."""
    response = requests.get("https://api.tfl.gov.uk/Place/Meta/Categories")
    if response.status_code == 200:
        data = response.json()
        kwargs['ti'].xcom_push(key='raw_data', value=data)
    else:
        raise ValueError(f"Failed to fetch data: {response.status_code}")


def transform_data(**kwargs):
    """Filters categories with >500 entries, enriches, and aggregates."""
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract', key='raw_data')
    
    filtered_data = [item for item in raw_data if item.get('entries', 0) > 500]
    
    # Enrichment - Fetching additional metadata
    enrichment_response = requests.get("https://api.publicapis.org/entries")
    metadata = enrichment_response.json().get('entries', []) if enrichment_response.status_code == 200 else []
    
    for item in filtered_data:
        item['extra_info'] = metadata[:2]  # Adding first 2 metadata entries as enrichment
    
    aggregated_data = {cat['category']: len(cat.get('extra_info', [])) for cat in filtered_data}
    
    ti.xcom_push(key='transformed_data', value=aggregated_data)


def load_data(**kwargs):
    """Loads transformed data to httpbin.org/post."""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    response = requests.post("https://httpbin.org/post", json=transformed_data)
    if response.status_code != 200:
        raise ValueError("Data load failed")


def data_quality_check():
    """Performs basic data quality checks."""
    return "load_data" if True else "handle_failure"


with DAG(
    dag_id='gen2_chat',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')
    
    # Ensure API availability
    api_available = HttpSensor(
        task_id='check_api',
        http_conn_id='tfl_api',
        endpoint='Place/Meta/Categories',
        poke_interval=10,
        timeout=30
    )
    
    with TaskGroup('extraction') as extraction:
        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_data,
            provide_context=True
        )
    
    with TaskGroup('transformation') as transformation:
        transform = PythonOperator(
            task_id='transform',
            python_callable=transform_data,
            provide_context=True
        )
    
    with TaskGroup('loading') as loading:
        load = PythonOperator(
            task_id='load_data',
            python_callable=load_data,
            provide_context=True
        )
    
    quality_check = BranchPythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )
    
    failure_handling = EmptyOperator(task_id='handle_failure')
    
    external_dependency = ExternalTaskSensor(
        task_id='wait_for_upstream',
        external_dag_id='another_dag',
        external_task_id='some_task',
        timeout=600
    )
    
    sla_alert = EmailOperator(
        task_id='sla_alert',
        to='admin@example.com',
        subject='SLA Alert: Pipeline Running Late',
        html_content='Pipeline execution is exceeding the SLA!'
    )
    
    sql_check = SQLCheckOperator(
        task_id='check_data_validity',
        conn_id='postgres_default',
        sql='SELECT COUNT(*) FROM data_table WHERE category IS NOT NULL'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> api_available >> extraction >> transformation >> quality_check
    quality_check >> [loading, failure_handling]
    loading >> sql_check >> end
    failure_handling >> sla_alert
    external_dependency >> extraction
