from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import requests
import json
import logging
from datetime import timedelta






import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'
# Constants
TFL_API_URL = "https://api.tfl.gov.uk/Place/Meta/Categories"
HTTPBIN_POST_URL = "https://httpbin.org/post"
METADATA_SOURCE_URL = "https://example.com/metadata"  # Replace with a real metadata source
DATABASE_CONN_ID = "my_db_connection"  # Replace with your DB connection ID

# Default arguments for the DAG
default_args = {
    "owner": "Chiara",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),  # SLA for the entire DAG
    "email_on_failure": True,
    "email_on_retry": False,
}

# Define the DAG
dag = DAG(
    "gen2_deepseek",
    default_args=default_args,
    description="A complex pipeline to extract, transform, and load TFL data",
    schedule_interval=None,
    catchup=False,
    tags=["tfl", "data_pipeline"],
)

# Task 1: Extract data from TFL API
def extract_tfl_data(**kwargs):
    try:
        response = requests.get(TFL_API_URL)
        response.raise_for_status()
        data = response.json()
        kwargs["ti"].xcom_push(key="tfl_data", value=data)
        logging.info("Successfully extracted TFL data.")
    except Exception as e:
        logging.error(f"Failed to extract TFL data: {e}")
        raise AirflowFailException("Extraction failed.")

extract_task = PythonOperator(
    task_id="extract_tfl_data",
    python_callable=extract_tfl_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Filter categories with >500 entries
def filter_categories(**kwargs):
    try:
        data = kwargs["ti"].xcom_pull(key="tfl_data")
        filtered_data = [category for category in data if category.get("availableEntries", 0) > 500]
        kwargs["ti"].xcom_push(key="filtered_data", value=filtered_data)
        logging.info(f"Filtered {len(filtered_data)} categories.")
    except Exception as e:
        logging.error(f"Failed to filter categories: {e}")
        raise AirflowFailException("Filtering failed.")

filter_task = PythonOperator(
    task_id="filter_categories",
    python_callable=filter_categories,
    provide_context=True,
    dag=dag,
)

# Task 3: Enrich data with additional metadata
def enrich_data(**kwargs):
    try:
        filtered_data = kwargs["ti"].xcom_pull(key="filtered_data")
        metadata_response = requests.get(METADATA_SOURCE_URL)
        metadata_response.raise_for_status()
        metadata = metadata_response.json()

        enriched_data = []
        for category in filtered_data:
            category_id = category.get("id")
            additional_info = metadata.get(category_id, {})
            category.update(additional_info)
            enriched_data.append(category)

        kwargs["ti"].xcom_push(key="enriched_data", value=enriched_data)
        logging.info("Successfully enriched data.")
    except Exception as e:
        logging.error(f"Failed to enrich data: {e}")
        raise AirflowFailException("Enrichment failed.")

enrich_task = PythonOperator(
    task_id="enrich_data",
    python_callable=enrich_data,
    provide_context=True,
    dag=dag,
)

# Task 4: Aggregate results by category type
def aggregate_data(**kwargs):
    try:
        enriched_data = kwargs["ti"].xcom_pull(key="enriched_data")
        aggregated_data = {}
        for category in enriched_data:
            category_type = category.get("type")
            if category_type not in aggregated_data:
                aggregated_data[category_type] = []
            aggregated_data[category_type].append(category)

        kwargs["ti"].xcom_push(key="aggregated_data", value=aggregated_data)
        logging.info("Successfully aggregated data.")
    except Exception as e:
        logging.error(f"Failed to aggregate data: {e}")
        raise AirflowFailException("Aggregation failed.")

aggregate_task = PythonOperator(
    task_id="aggregate_data",
    python_callable=aggregate_data,
    provide_context=True,
    dag=dag,
)

# Task 5: Data quality check using SQLCheckOperator
data_quality_check = SQLCheckOperator(
    task_id="data_quality_check",
    sql="SELECT COUNT(*) FROM my_table WHERE entries > 500",  # Replace with your SQL query
    conn_id=DATABASE_CONN_ID,
    dag=dag,
)

# Task 6: Load data to httpbin.org/post using HttpOperator
load_task = HttpOperator(
    task_id="load_data",
    method="POST",
    endpoint="post",  # The endpoint for httpbin.org/post
    data="{{ ti.xcom_pull(key='aggregated_data') }}",  # Use XCom to pull data
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 200,  # Check for success
    dag=dag,
)

# Task 7: Fallback mechanism (e.g., log failure and notify)
def fallback_mechanism(**kwargs):
    logging.error("Pipeline failed. Initiating fallback mechanism.")
    # Add notification logic here (e.g., send email or Slack message)

fallback_task = PythonOperator(
    task_id="fallback_mechanism",
    python_callable=fallback_mechanism,
    trigger_rule="one_failed",  # Run if any upstream task fails
    dag=dag,
)

# Task Group for Transformation Steps
with TaskGroup(group_id="transformation_group", dag=dag) as transformation_group:
    filter_task >> enrich_task >> aggregate_task

# External Task Sensor (wait for another DAG to complete)
external_sensor = ExternalTaskSensor(
    task_id="wait_for_external_dag",
    external_dag_id="another_dag_id",  # Replace with the actual DAG ID
    external_task_id="another_task_id",  # Replace with the actual task ID
    mode="reschedule",
    dag=dag,
)

# Branching logic based on data quality
def decide_next_step(**kwargs):
    data_quality = kwargs["ti"].xcom_pull(task_ids="data_quality_check")
    if data_quality:
        return "load_data"
    else:
        return "fallback_mechanism"

branch_task = BranchPythonOperator(
    task_id="branch_on_data_quality",
    python_callable=decide_next_step,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_task >> external_sensor >> transformation_group >> data_quality_check >> branch_task
branch_task >> [load_task, fallback_task]