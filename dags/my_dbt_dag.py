"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator

# adjust for other database types
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from pendulum import datetime
import os

YOUR_NAME = "marco"
CONNECTION_ID = "redshift-datalake-prod"
DB_NAME = "prod"
SCHEMA_NAME = "public"
MODEL_TO_QUERY = "campaign_performance"
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
        profile_args={"schema": SCHEMA_NAME,
                      "port": 3921,
                      },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


def get_df_from_db():
    hook = RedshiftSQLHook(redshift_conn_id=CONNECTION_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {DB_NAME}.{SCHEMA_NAME}.{MODEL_TO_QUERY} limit 10")
    records = cursor.fetchall()
    return records

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
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"my_name": "{{ params.my_name }}" }',
        },
        default_args={"retries": 2},
    )

    query_table = PythonOperator(
        task_id="query_table",
        python_callable=get_df_from_db,
        provide_context=True,
    )

    transform_data >> query_table


my_simple_dbt_dag()