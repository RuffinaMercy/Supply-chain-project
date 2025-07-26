# # dags/supply_chain_pipeline_dag.py
# from __future__ import annotations
# import pendulum
# from airflow.models.dag import DAG
# from airflow.operators.bash import BashOperator

# # Define the directory where your project is mounted inside the container
# PROJECT_DIR = "/opt/airflow/project"
# DBT_PROJECT_DIR = f"{PROJECT_DIR}/project_main"

# with DAG(
#     dag_id="m5_supply_chain_pipeline",
#     start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
#     schedule=None,
#     catchup=False,
#     tags=["m5", "dbt", "snowpark"],
# ) as dag:
#     # Task 1: Run dbt models
#     dbt_run_task = BashOperator(
#         task_id="dbt_run",
#         bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
#     )

#     # Task 2: Run the Snowpark ML pipeline
#     ml_pipeline_task = BashOperator(
#         task_id="run_ml_pipeline",
#         bash_command=f"cd {PROJECT_DIR} && python ml_pipeline.py",
#     )

#     # Define the dependency (the order of the steps)
#     dbt_run_task >> ml_pipeline_task








# dags/supply_chain_pipeline_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/project_main"

# This long bash command creates the profiles.yml on the fly
# It reads the environment variables that Docker Compose injects from the .env file
DBT_RUN_COMMAND = f"""
mkdir -p /home/airflow/.dbt && \
echo "
default:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: $SNOWFLAKE_ACCOUNT
      user: $SNOWFLAKE_USER
      password: $SNOWFLAKE_PASSWORD
      role: $SNOWFLAKE_ROLE
      database: $SNOWFLAKE_DATABASE
      warehouse: $SNOWFLAKE_WAREHOUSE
      schema: $SNOWFLAKE_SCHEMA
      threads: 4
" > /home/airflow/.dbt/profiles.yml && \
cd {DBT_PROJECT_DIR} && \
dbt run
"""

with DAG(
    dag_id="m5_supply_chain_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["m5", "dbt", "snowpark"],
) as dag:
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_RUN_COMMAND,
    )

    ml_pipeline_task = BashOperator(
        task_id="run_ml_pipeline",
        bash_command=f"cd {PROJECT_DIR} && python ml_pipeline.py",
    )

    dbt_run_task >> ml_pipeline_task