from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import json

# This should be parameterized in an operator
GLOBAL_CLI_FLAGS = f"--no-write-json"


class DbtDagParser:
    """
    A utility class to parse out a dbt project and return Airflow tasks

    :param dag: tbd
    :param dbt_project_dir: tbd
    :param dbt_profiles_dir: tbd
    """

    def __init__(self,
                 dag=None,
                 dbt_project_dir=None,
                 dbt_profiles_dir=None,
                 dbt_target='dev',
                 dbt_tag=None
                 ):
        self.dag = dag
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.dbt_target = dbt_target
        self.dbt_tag = dbt_tag

    def load_dbt_manifest(self):
        """
        Returns: a json object containing the dbt manifest content
        """
        import os
        manifest_path = os.path.join(self.dbt_project_dir, 'target/manifest.json')
        with open(manifest_path) as f:
            file_content = json.load(f)
        return file_content

    def make_dbt_task(self, node_name, dbt_verb, task_group):
        """

        Args:
            node_name:
            dbt_verb:
            task_group:

        Returns:

        """

        model = node_name.split(".")[-1]
        if dbt_verb == "test":
            # Just a cosmetic renaming of the node
            node_name = node_name.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_name,
            task_group=task_group,
            bash_command=f"""
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target {self.dbt_target} --models {model} \
            --profiles-dir {self.dbt_profiles_dir} --project-dir {self.dbt_project_dir} 
            """,
            dag=self.dag,
        )
        print('Created task:', node_name)
        return dbt_task

    # TODO: dbt target needs to be parameterized too
    def make_dbt_task_group(self, upstream_task, downstream_task, include_tests=True):
        """

        Args:
            upstream_task:
            downstream_task:

        Returns:

        """
        # TODO: do the compile somewhere
        manifest_json = self.load_dbt_manifest()

        dbt_tasks = {}

        dbt_run_group = TaskGroup('dbt_run')
        dbt_test_group = TaskGroup('dbt_test')

        # Create the tasks
        for node_name in manifest_json["nodes"].keys():
            if node_name.split(".")[0] == "model":

                # Make the run nodes
                dbt_tasks[node_name] = self.make_dbt_task(
                    node_name,
                    "run",
                    dbt_run_group)

                # Make the test nodes
                if include_tests:
                    # TODO: check what the model>test renaming/node_name exactly does, I think this can be simplified
                    node_test = node_name.replace("model", "test")
                    dbt_tasks[node_test] = self.make_dbt_task(
                        node_name,
                        "test",
                        dbt_test_group
                    )

        # Add upstream and downstream dependencies for each task
        for node_name in manifest_json["nodes"].keys():
            if node_name.split(".")[0] == "model":
                if include_tests:
                    # Set dependency to run tests on a model after model runs finishes
                    node_test = node_name.replace("model", "test")
                    # Make sure these have the "end" task as the downstream dependency
                    dbt_tasks[node_name] >> dbt_tasks[node_test] >> downstream_task
                else:
                    dbt_tasks[node_name] >> downstream_task
                # Set all model -> model dependencies
                for upstream_node in manifest_json["nodes"][node_name]["depends_on"]["nodes"]:
                    upstream_node_type = upstream_node.split(".")[0]
                    if upstream_node_type == "model":
                        # Make sure the dbt_compile node is an upstream here
                        upstream_task >> dbt_tasks[upstream_node] >> dbt_tasks[node_name]

        return dbt_tasks
