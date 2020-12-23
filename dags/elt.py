import datetime
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 23),
    'email': ['noreply@astronomer.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'elt_dag',
    default_args=default_args,
    description='A mock ELT pipeline that mocks implementation for singer taps, targets, and dbt.',
    schedule_interval=timedelta(days=1),
)

with dag:
    run_jobs = TaskGroup("dbt_run")
    test_jobs = TaskGroup('dbt_test')

start = DummyOperator(task_id='start', dag=dag)
# Github Singer Tap for data extract. Note that this is a mocked help command at the moment.
extract = BashOperator(task_id='singer_tap_extract', bash_command="tap-github -h", dag=dag)
# CSV Singer Target for data loading. Note that this is a mocked help command at the moment.
load = BashOperator(task_id='singer_target_load', bash_command="target-csv -h", dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> extract >> load

def load_manifest():
    local_filepath = "/usr/local/airflow/dags/dbt/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator to run or test an individual model"""
    DBT_DIR = "/usr/local/airflow/dags/dbt"
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]
    with dag:
        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                task_group=run_jobs,
                bash_command=f"""
                cd {DBT_DIR} &&
                dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
                """,
            )
        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node_test,
                task_group=test_jobs,
                bash_command=f"""
                cd {DBT_DIR} &&
                dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
                """,
            )
    return dbt_task


data = load_manifest()
dbt_tasks = {}
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")
        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test] >> end
        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                load >> dbt_tasks[upstream_node] >> dbt_tasks[node]
