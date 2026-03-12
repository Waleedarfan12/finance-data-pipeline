from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="daily_etl_pipeline",
    start_date=datetime(2026, 3, 10),
    schedule_interval="@daily",   # runs once per day
    catchup=False,
    tags=["etl", "finance"],
) as dag:

    # Ingest step
    ingest = BashOperator(
        task_id="ingest_data",
        bash_command=" python /opt/airflow/spark_jobs/ingest_data.py"
    )

    # Transform step
    transform = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/spark_jobs/transform_data.py"
    )

    # Load step
    load = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/spark_jobs/load_data.py"
    )

    # Task dependencies
    ingest >> transform >> load