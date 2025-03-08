# My ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline fetches data from an API, performs basic transformations, and posts the results to another API.

## Project Structure

```
my_etl_pipeline
├── dags
│   └── etl_dag.py               # Defines the Airflow DAG for the ETL pipeline
├── scripts
│   └── transformation_script.py   # Contains the transformation logic for the data
├── config
│   └── config.yaml               # Holds configuration settings for the ETL pipeline
├── tests
│   └── test_etl_dag.py           # Contains unit tests for the DAG
├── README.md                      # Documentation for the project
└── requirements.txt               # Lists the Python dependencies required for the project
```

## Setup Instructions

1. **Clone the repository**:
   ```
   git clone <repository-url>
   cd my_etl_pipeline
   ```

2. **Install dependencies**:
   It is recommended to create a virtual environment before installing the dependencies.
   ```
   pip install -r requirements.txt
   ```

3. **Configure the pipeline**:
   Update the `config/config.yaml` file with any necessary configuration settings, such as API endpoints.

4. **Run the Airflow scheduler**:
   Start the Airflow scheduler and web server to monitor the DAG.
   ```
   airflow scheduler
   airflow webserver
   ```

5. **Trigger the DAG**:
   Access the Airflow web interface and manually trigger the `etl_dag`.

## Usage

The ETL pipeline will:
- Fetch data from the TFL API.
- Transform the data using the logic defined in `scripts/transformation_script.py`.
- Post the transformed data to the specified endpoint.

## Testing

Unit tests for the DAG can be run using:
```
pytest tests/test_etl_dag.py
```

## License

This project is licensed under the MIT License.