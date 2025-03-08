from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define your Python functions (tasks)
def print_hello():
    print("Hello, Airflow!")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 18),  # Start date for the DAG
    'retries': 1,  # Number of retries on failure
}

# Define the DAG
dag = DAG(
    'my_first_dag',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval=None,  # Change this to a cron string if you want periodic runs
)

# Define the tasks
task1 = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Set the task dependencies
task1  # In this case, just a single task

