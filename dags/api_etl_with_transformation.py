import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from _scproxy import _get_proxy_settings

_get_proxy_settings()
os.environ['NO_PROXY'] = '*'

# Define the API URLs
API_URL_READ = "https://jsonplaceholder.typicode.com/posts"  # API to read from
API_URL_WRITE = "https://httpbin.org/post"  # API to write to (same API for this example)

# ETL Function for GET request (Extract)
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

# ETL Function for Transformation
# ETL Function for Transformation
def transform(data):
    print("Transforming data...")
    transformed_data = []
    
    for item in data:
        # Ensure the item is a dictionary and contains 'title' and 'body'
        if isinstance(item, dict) and 'title' in item and 'body' in item:
            transformed_item = {
                'title': item['title'],  # Keep original title
                'body': f"Start of post: {item['body']} -- End of post.",  # Add prefix and suffix to body
                'summary': f"{item['title']} | {item['body'][:50]}...",  # Concatenate title and body for a short summary
            }
            
            # Add only if title is longer than 20 characters
            if len(item['title']) > 20:
                transformed_data.append(transformed_item)
            else:
                print(f"Skipping item with short title: {item['title']}")
        else:
            print(f"Skipping invalid item: {item}")
    
    print("Transformation complete!")
    return transformed_data


# ETL Function for POST request (Load)
def load(data):
    print("Starting to load the transformed data...")
    try:
        response = requests.post(API_URL_WRITE, json=data, timeout=10)  # POST request to write the data
        response.raise_for_status()  # Raise an exception for 4xx/5xx errors
        print("Data loaded successfully!")
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
    'api_etl_with_transformation',
    description='DAG with extraction, transformation, and loading data with a GET and POST request',
    schedule_interval=None,
    start_date=datetime(2025, 3, 7),
    catchup=False,
    default_args=default_args  # Apply default_args to the DAG
)

# Define the tasks using PythonOperator
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    op_args=['{{ task_instance.xcom_pull(task_ids="extract") }}'],
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform") }}'],
    provide_context=True,
    dag=dag,
)

# Set the task sequence
extract_task >> transform_task >> load_task
