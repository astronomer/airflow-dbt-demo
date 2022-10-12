"""
A basic dbt DAG that shows how to run dbt commands via the BashOperator

Follows the standard dbt seed, run, and test pattern.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"


with DAG(
    "dbt_basic_dag",
    start_date=datetime(2020, 12, 23),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval=None,
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": "{{ conn.postgres.login }}",
            "DBT_ENV_SECRET_PASSWORD": "{{ conn.postgres.password }}",
            "DBT_HOST": "{{ conn.postgres.host }}",
            "DBT_SCHEMA": "{{ conn.postgres.schema }}",
            "DBT_PORT": "{{ conn.postgres.port }}",
        }
    },
) as dag:
    # This task loads the CSV files from dbt/data into the local postgres database for the purpose of this demo.
    # In practice, we'd usually expect the data to have already been loaded to the database.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_seed >> dbt_run >> dbt_test
