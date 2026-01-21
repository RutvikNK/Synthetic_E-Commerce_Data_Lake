import os

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# If you need to set specific paths for dbt (optional but good for stability)
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/transformation")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_analytics_pipeline',
    default_args=default_args,
    description='End-to-end: Ingest -> Marts -> Reports',
    # UPDATED: 'schedule_interval' is deprecated in favor of 'schedule'
    schedule='@daily', 
    start_date=datetime(2025, 1, 1), # Fixed dates are better than days_ago() for stability
    catchup=False,
    tags=['ecommerce', 'dbt'],
) as dag:

    # Update Seeds
    # We use cd to ensure we are in the correct directory before running uv/dbt
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_DIR} && uv run dbt seed',
    )

    # Build the Core Data Layers (Staging + Marts)
    # Excluding reporting to avoid building reports before tests
    dbt_build_marts = BashOperator(
        task_id='dbt_build_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && uv run dbt run --exclude models/reporting',
    )

    # Test the Core Layers
    # Pipeline stops if this fails
    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && uv run dbt test --exclude models/reporting',
    )

    # Generate the Final Reports
    # This only runs if the previous tests passed
    dbt_build_reports = BashOperator(
        task_id='dbt_build_reports',
        bash_command=f'cd {DBT_PROJECT_DIR} && uv run dbt run --select models/reporting',
    )

    # Define the Dependency Chain
    dbt_seed >> dbt_build_marts >> dbt_test_marts >> dbt_build_reports