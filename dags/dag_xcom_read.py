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
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from src.secret import get_secret

DBT_BIN = "/usr/local/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/profile"
DBT_DEFAULTS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

# Make Snowflake credentials available in the Airflow env
# so that DBT can use them for variable substitution in the profile
#SNOWFLAKE_SECRET_NAME = "/sl-analytics/dev/airflow/connections/snowflake_credentials"
#env_vars = get_secret(SNOWFLAKE_SECRET_NAME)
#for e in env_vars:
#    os.environ[e] = env_vars[e]

DEFAULT_ARGS = {
    "owner": "airflow",
    'start_date': datetime(1970,1,1),
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": "snowflake",
}

TRIGGER_ARGS = {
    "starting_day": "2021-12-06",
    "end_day": "2021-12-08",
    "database": "dwh_piotr",
    "warehouse": "compute_wh",
    "schema": "raw",
    "role": "sysadmin"
}

#Parameter
starting_day = "{{ dag_run.conf['starting_day'] if dag_run.conf.get('starting_day') else '2021-11-01' }}"
end_day = "{{ dag_run.conf['end_day'] if dag_run.conf.get('end_day') else '2021-11-30' }}"
database="{{ dag_run.conf['database'] if dag_run.conf.get('database') else 'dwh_piotr' }}"
warehouse="{{ dag_run.conf['warehouse'] if dag_run.conf.get('warehouse') else 'sac_event_wh' }}"
schema = "{{ dag_run.conf['schema'] if dag_run.conf.get('schema') else 'raw' }}"
role = "{{ dag_run.conf['role'] if dag_run.conf.get('role') else 'sysadmin' }}"


log = logging.getLogger(__name__)


def test1(parent_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.test1' % parent_dag_name,
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    def return_list():
        return ['test1', 'test2']

    list_extract_folder = PythonOperator(
        task_id='list',
        dag=dag,
        python_callable=return_list
    )

    clean_xcoms = PostgresOperator(
        task_id='clean_xcoms',
        postgres_conn_id='airflow_db',
        sql="delete from xcom where dag_id='{{ dag.dag_id }}'",
        dag=dag)

    clean_xcoms >> list_extract_folder

    return dag

def test2(parent_dag_name, start_date, schedule_interval, parent_dag=None):
    dag = DAG(
        '%s.test2' % parent_dag_name,
        schedule_interval=schedule_interval,
        start_date=start_date
    )

    if len(parent_dag.get_active_runs()) > 0:
        test_list = parent_dag.get_task_instances(session=Session, start_date=parent_dag.get_active_runs()[-1])[-1].xcom_pull(
            dag_id='%s.%s' % (parent_dag_name, 'test1'),
            task_ids='list')
        if test_list:
            for i in test_list:
                test = DummyOperator(
                    task_id=i,
                    dag=dag
                )

    return dag

DAG_NAME = 'dag_xcom_read'

dag = DAG(DAG_NAME,
          description='Test workflow',
          catchup=False,
          schedule_interval=None,
          tags=['test','xcom'],
          start_date=datetime(2022, 2, 3))

test1 = SubDagOperator(
    subdag=test1(DAG_NAME,
                 dag.start_date,
                 dag.schedule_interval),
    task_id='test1',
    dag=dag
)

test2 = SubDagOperator(
    subdag=test2(DAG_NAME,
                 dag.start_date,
                 dag.schedule_interval,
                 parent_dag=dag),
    task_id='test2',
    dag=dag
)

test1 >> test2
