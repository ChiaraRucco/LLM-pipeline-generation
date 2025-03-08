from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.sql import SQLCheckOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
import logging
import json
from datetime import datetime, timedelta



import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'


default_args = {
    'owner': 'Chiara',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=15)
}

def extract_data(**kwargs):
    """Extract categories from TFL API"""
    url = "https://api.tfl.gov.uk/Place/Meta/Categories"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    kwargs['ti'].xcom_push(key='raw_data', value=data)
    logging.info(f"Extracted {len(data)} categories")

def filter_categories(**kwargs):
    """Filter categories with >500 entries"""
    ti = kwargs['ti']
    data = ti.xcom_pull(key='raw_data', task_ids='extract_group.extract')
    filtered = [item for item in data if item.get('entryCount', 0) > 500]
    ti.xcom_push(key='filtered_data', value=filtered)
    logging.info(f"Filtered to {len(filtered)} categories")

def enrich_metadata(**kwargs):
    """Enrich with Wikipedia summaries"""
    ti = kwargs['ti']
    data = ti.xcom_pull(key='filtered_data', task_ids='transform_group.filter')
    
    for item in data:
        # Mock enrichment with Wikipedia API (example)
        try:
            wiki_response = requests.get(
                "https://en.wikipedia.org/api/rest_v1/page/summary/" + item['category']
            )
            wiki_data = wiki_response.json()
            item['description'] = wiki_data.get('extract', 'No description found')
        except Exception as e:
            logging.warning(f"Enrichment failed for {item['category']}: {str(e)}")
            item['description'] = 'Enrichment failed'
            
    ti.xcom_push(key='enriched_data', value=data)
    logging.info("Enrichment completed")

def aggregate_results(**kwargs):
    """Aggregate by category type"""
    ti = kwargs['ti']
    data = ti.xcom_pull(key='enriched_data', task_ids='transform_group.enrich')
    
    aggregation = {}
    for item in data:
        category_type = item.get('type', 'Unknown')
        aggregation[category_type] = {
            'total_entries': item['entryCount'],
            'categories': aggregation.get(category_type, {}).get('categories', []) + [item['category']]
        }
        
    ti.xcom_push(key='aggregated_data', value=aggregation)
    logging.info(f"Aggregated results: {json.dumps(aggregation, indent=2)}")

def load_to_db(**kwargs):
    """Load to SQLite for data quality checks"""
    import sqlite3
    ti = kwargs['ti']
    data = ti.xcom_pull(key='aggregated_data', task_ids='transform_group.aggregate')
    
    conn = sqlite3.connect('/tmp/tfl_data.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS results (category_type TEXT, total_entries INTEGER)")
    
    for category_type, values in data.items():
        cursor.execute("INSERT INTO results VALUES (?, ?)", 
                      (category_type, values['total_entries']))
                      
    conn.commit()
    conn.close()
    logging.info("Data loaded to SQLite for validation")

def check_data_quality(**kwargs):
    """Check if data meets minimum requirements"""
    ti = kwargs['ti']
    data = ti.xcom_pull(key='aggregated_data', task_ids='transform_group.aggregate')
    return len(data) > 0 and all(v['total_entries'] > 0 for v in data.values())

def branch_based_on_quality(**kwargs):
    """Determine next step based on data quality"""
    if check_data_quality(**kwargs):
        return 'load_group.load_to_httpbin'
    return 'load_group.data_quality_alert'

def load_to_httpbin(**kwargs):
    """Post results to httpbin.org with error handling"""
    ti = kwargs['ti']
    data = ti.xcom_pull(key='aggregated_data', task_ids='transform_group.aggregate')
    
    try:
        response = requests.post("https://httpbin.org/post", json=data)
        response.raise_for_status()
        logging.info(f"Load successful: {response.status_code}")
    except requests.exceptions.HTTPError as e:
        if 500 <= e.response.status_code < 600:
            logging.warning("Server error, retrying...")
            raise
        else:
            logging.error(f"Client error: {str(e)}")
            raise

with DAG('gen2_qwen', 
         schedule_interval=None,
         default_args=default_args,
         catchup=False) as dag:

    # External dependency sensor
    wait_for_dependency = ExternalTaskSensor(
        task_id='wait_for_dependency',
        external_dag_id='upstream_dag',
        external_task_id='finalize',
        timeout=3600
    )

    with TaskGroup('extract_group') as extract_group:
        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_data
        )

    with TaskGroup('transform_group') as transform_group:
        filter_task = PythonOperator(
            task_id='filter',
            python_callable=filter_categories
        )
        
        enrich_task = PythonOperator(
            task_id='enrich',
            python_callable=enrich_metadata
        )
        
        aggregate_task = PythonOperator(
            task_id='aggregate',
            python_callable=aggregate_results
        )
        
        filter_task >> enrich_task >> aggregate_task

    with TaskGroup('load_group') as load_group:
        load_db = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db
        )
        
        quality_check = SQLCheckOperator(
            task_id='sql_quality_check',
            conn_id='sqlite_default',
            sql="SELECT COUNT(*) FROM results WHERE total_entries > 0"
        )
        
        branch = BranchPythonOperator(
            task_id='branch',
            python_callable=branch_based_on_quality
        )
        
        load_httpbin = PythonOperator(
            task_id='load_to_httpbin',
            python_callable=load_to_httpbin
        )
        
        alert = DummyOperator(
            task_id='data_quality_alert'
        )
        
        load_db >> quality_check >> branch >> [load_httpbin, alert]

    # DAG structure
    wait_for_dependency >> extract_group >> transform_group >> load_group

    # SLA monitoring configuration
    dag.get_task('extract_group.extract').set_sla(timedelta(minutes=2))
    dag.get_task('load_group.load_to_httpbin').set_sla(timedelta(minutes=5))