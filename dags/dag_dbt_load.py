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
from airflow import settings
from airflow.models import Variable
from airflow.settings import Session
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models.connection import Connection
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.db import provide_session, merge_conn
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
    "dag_dbt_load",
    default_args=DEFAULT_ARGS,
    description="DBT load",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 16),
    #end_date=datetime(2022, 10, 16),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "debug"],
) as dag:
    
    @provide_session
    def store_conn(conn, session=None):
        from airflow.models import Connection
        if session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
            logging.info("Connection object already exists, attempting to remove it...")
            session.delete(session.query(Connection).filter(Connection.conn_id == conn.conn_id).first())

        session.add(conn)
        session.commit()

    day_partitioning = "{{execution_date.strftime('%Y/%m/%d')}}"
    loaded_at = "{{execution_date.strftime('%Y-%m-%d')}}"
    
    new_conn = Connection( conn_id="postgres_connect", conn_type="postgres", description="connection to pg", host="postgres", schema="public", login="airflow")
    new_conn.set_password("airflow")
    merge_conn(new_conn)

    #postgres_conn_id = "postgres_default"
    postgres_conn_id = "postgres_connect"
    database = "airflow"

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BIN} deps {DBT_DEFAULTS}",
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_BIN} debug {DBT_DEFAULTS}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run -m models/example/my_first_dbt_model.sql {DBT_DEFAULTS}",
    )

    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"{DBT_BIN} clean {DBT_DEFAULTS}",
    )

    insert_into_table = PostgresOperator(
        task_id="insert_into_table",
        sql=f"insert into my_first_dbt_model(id) values(2);",
        postgres_conn_id = postgres_conn_id,
        database = database
    )

    update_table = PostgresOperator(
        task_id="update_table",
        sql=f"update my_first_dbt_model set id = 3 where id = 1;",
        postgres_conn_id = postgres_conn_id,
        database = database
    )

    (
        dbt_deps
        >> dbt_debug
        >> dbt_run
        >> dbt_clean
        >> insert_into_table
        >> update_table
    )
