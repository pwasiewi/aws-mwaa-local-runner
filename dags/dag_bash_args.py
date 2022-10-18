# https://dev.to/aws/working-with-parameters-and-variables-in-amazon-managed-workflows-for-apache-airflow-4f5h
# https://big-data-demystified.ninja/2020/04/15/airflow-xcoms-example-airflow-demystified/
# https://fares.codes/posts/everything-you-probably-need-to-know-about-airflow/
# https://groups.google.com/g/airbnb_airflow/c/GRdoW30PNUI
# https://www.cloudwalker.io/2019/07/29/airflow-sub-dags/
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

DEFAULT_ARGS = {
    "owner": "airflow",
    'start_date': datetime(2022,10,18),
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": "snowflake",
}

TRIGGER_ARGS = {
    "starting_day": "2022-10-17",
    "end_day": "2022-10-18",
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

run_this="echo "+database+"."+warehouse

def test_func(database,warehouse,day):
    print(database+"."+warehouse+"."+day)
    mydate = datetime.strptime(day, '%Y-%m-%d')
    day2 = mydate.strftime('%Y/%m/%d')
    print("\nPo konwersji: "+day2)


with DAG(
    dag_id="dag_bash_args",
    schedule_interval=None,
    catchup=False,
    tags=['BashOperator', 'PythonOperator', 'args', 'DEFAULT_ARGS', 'TRIGGER_ARGS'],
    start_date=days_ago(1)) as dag:

    get_var_filename = BashOperator(
        task_id="get_var_filename",
        bash_command=f"""echo 'You are running this DAG with {warehouse} and the following variable file: {starting_day} '""",
    )
    get_var_filename = BashOperator(
        task_id="get_var_filename2",
        bash_command=f'echo "You are running this DAG with {warehouse} and the following variable file: \'{starting_day}\'"',
    )
    get_var_filename = BashOperator(
        task_id="get_var_filename3",
        bash_command=f'echo "You are running this DAG with {warehouse} and the following variable file: \'{starting_day}\'"',
    )

    task1 = BashOperator(
        task_id='test_task1',
        bash_command=run_this,
    )

    task2 = PythonOperator(
        task_id='test_task2',
        python_callable=test_func,
        op_kwargs={"database": database, "warehouse": warehouse, "day": starting_day},
    )
