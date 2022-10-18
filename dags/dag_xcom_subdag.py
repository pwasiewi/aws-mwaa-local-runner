# https://dev.to/aws/working-with-parameters-and-variables-in-amazon-managed-workflows-for-apache-airflow-4f5h
# https://fares.codes/posts/everything-you-probably-need-to-know-about-airflow/
# https://www.cloudwalker.io/2019/07/29/airflow-sub-dags/
# https://big-data-demystified.ninja/2020/04/15/airflow-xcoms-example-airflow-demystified/
import os
from typing import Sequence
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DBT_BIN = "/usr/local/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/"
# DBT_PROJECT_DIR = "/usr/local/airflow/dags/piotr/"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/profile"
# DBT_PROFILES_DIR = "/usr/local/airflow/dags/piotr/profile"
DBT_DEFAULTS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

DEFAULT_ARGS = {
    "owner": "airflow",
    'start_date': days_ago(2),
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
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

def use_pushed_val(pushed_val, ds, **kwargs):
    print(pushed_val)
    return pushed_val

def Sub_Dag1(parent_dag_name, child_dag_name, start_date, schedule_interval, pushed_val):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    start_date=start_date,
  )

  childTask = PythonOperator(
      task_id='child_task',
      python_callable=use_pushed_val,
      op_kwargs = {'pushed_val' : pushed_val},
      provide_context=True,
      dag=dag
    )

  return dag

#parent_dag.py

PARENT_DAG_NAME = "dag_xcom_subdag_parent"
CHILD_DAG_NAME = "dag_xcom_subdag_child"

main_dag = DAG(
  dag_id=PARENT_DAG_NAME,
  schedule_interval=None,
  start_date=datetime(2022, 10, 18),
  tags=['xcom','PythonOperator','push'],
)


def push_value(**kwargs):
    ''' push into Xcom '''
    return [1, 2]

t1 = PythonOperator(task_id='push_value',
                       python_callable=push_value,
                       retries=3,
                       dag=main_dag)

subdag_1 = SubDagOperator(
  subdag=Sub_Dag1(
      PARENT_DAG_NAME,
      CHILD_DAG_NAME,
      main_dag.start_date,
      main_dag.schedule_interval,
      "'{{ ti.xcom_pull(task_ids='push_value', dag_id='" + PARENT_DAG_NAME + "' )}}'"
  ),
  task_id=CHILD_DAG_NAME,
  dag=main_dag,
)
t1 >> subdag_1
