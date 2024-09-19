"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

# adjust for other database types
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from pendulum import datetime
import os

YOUR_NAME = "marco"
CONNECTION_ID = "redshift-datalake-prod"
DB_NAME = "prod"
SCHEMA_NAME = "public"
MODEL_TO_QUERY = "model2"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/my_simple_dbt_project"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    params={"my_name": YOUR_NAME},
)
def my_simple_dbt_dag():
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(
            DBT_PROJECT_PATH,
            skip_project_validation=True,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"my_name": "{{ params.my_name }}" }',
        },
        default_args={"retries": 2},
    )

    query_table = RedshiftSQLOperator(
        task_id="query_table",
        redshift_conn_id=CONNECTION_ID,
        sql=f"SELECT * FROM {DB_NAME}.{SCHEMA_NAME}.{MODEL_TO_QUERY}",
    )

    transform_data >> query_table


my_simple_dbt_dag()