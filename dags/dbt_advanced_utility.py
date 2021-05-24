from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup
from dbt_dag_parser import DbtDagParser

DBT_PROJECT_DIR = '/usr/local/airflow/dbt'
# DBT_PROJECT_DIR = '/Users/sam/code/airflow-dbt-demo/dbt/'

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 23),
    'email': ['noreply@astronomer.io'],
    'email_on_failure': False
}

dag = DAG(
    'dbt_advanced_dag_utility',
    default_args=default_args,
    description='A dbt wrapper for Airflow using a utility method to parse the dbt DAG',
    schedule_interval=None,
)

start_dummy = DummyOperator(task_id='start', dag=dag)
dbt_compile = DummyOperator(task_id='dbt_compile', dag=dag)
end_dummy = DummyOperator(task_id='end', dag=dag)

start_dummy >> dbt_compile

with dag:
    dag_parser = DbtDagParser(dag=dag,
                              dbt_project_dir=DBT_PROJECT_DIR,
                              dbt_profiles_dir=DBT_PROJECT_DIR,
                              dbt_target='dev',
                              dbt_tag=None
                              )
    dbt_task_group = dag_parser.make_dbt_task_group(
        upstream_task=dbt_compile,
        downstream_task=end_dummy
    )
