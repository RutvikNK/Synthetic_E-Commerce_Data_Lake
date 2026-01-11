import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# This gets the folder where THIS file lives: .../orchestration/dags
dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(dag_folder))

dbt_folder = os.path.join(project_root, "transformation")

# print it to logs
print(f"DBT Folder Path: {dbt_folder}")

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Disable retries for faster debugging
}

with DAG(
    'ecommerce_daily_transform',
    default_args=default_args,
    description='Runs dbt transformation daily',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce'],
) as dag:

    start_task = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting pipeline..."',
    )

    # Task 2: Run dbt
    # We inject the absolute path {dbt_folder} into the command
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {dbt_folder} && dbt run --profiles-dir .',
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {dbt_folder} && dbt test --profiles-dir .',
    )

    start_task >> dbt_run >> dbt_test