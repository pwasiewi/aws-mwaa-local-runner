# https://dev.to/aws/working-with-parameters-and-variables-in-amazon-managed-workflows-for-apache-airflow-4f5h
# https://stackoverflow.com/questions/41517798/proper-way-to-create-dynamic-workflows-in-airflow
# https://big-data-demystified.ninja/2020/04/15/airflow-xcoms-example-airflow-demystified/
# https://fares.codes/posts/everything-you-probably-need-to-know-about-airflow/
# https://groups.google.com/g/airbnb_airflow/c/GRdoW30PNUI
# https://www.cloudwalker.io/2019/07/29/airflow-sub-dags/

import os
import json
import logging
from typing import Sequence
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.settings import Session
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


DBT_BIN = "/usr/local/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/profile"
DBT_DEFAULTS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

"""
Default/constants used in sla DWH dags.
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": "snowflake",
}


with DAG(
    "dag_dbt_debug",
    default_args=DEFAULT_ARGS,
    description="DBT debug",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 1),
    end_date=datetime(2022, 7, 30),
    catchup=True,
    max_active_runs=1,
    tags=['dbt','BashOperator','debug','test'],
) as dag:
    
    # partitioning and loaded_at for the procedure calls
    day_partitioning = "{{execution_date.strftime('%Y/%m/%d')}}"
    loaded_at = "{{execution_date.strftime('%Y-%m-%d')}}"

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BIN} deps {DBT_DEFAULTS}",
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_BIN} debug {DBT_DEFAULTS}",
    )

    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"{DBT_BIN} clean {DBT_DEFAULTS}",
    )

    (
        dbt_deps  >> dbt_debug  >> dbt_clean
    )
