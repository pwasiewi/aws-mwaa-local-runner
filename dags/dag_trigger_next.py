# https://dev.to/aws/working-with-parameters-and-variables-in-amazon-managed-workflows-for-apache-airflow-4f5h
# https://stackoverflow.com/questions/41517798/proper-way-to-create-dynamic-workflows-in-airflow
# https://big-data-demystified.ninja/2020/04/15/airflow-xcoms-example-airflow-demystified/
# https://fares.codes/posts/everything-you-probably-need-to-know-about-airflow/
# https://groups.google.com/g/airbnb_airflow/c/GRdoW30PNUI
# https://www.cloudwalker.io/2019/07/29/airflow-sub-dags/
import os
import json
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

"""
DAG that can be used to trigger multiple other dags.
For example, trigger with the following config:
{
    "task_list": ["print_output","print_output"],
    "conf_list": [
        {
            "output": "Hello"
        },
        {
            "output": "world!"
        }
    ]
}
"""



dag_id = 'dag_trigger_branch_recursive'
branch_id = 'dag_trigger_branch_operator'
repeat_task_id = 'dag_trigger_repeat_dag_operator'
repeat_task_conf = repeat_task_id + '_conf'
next_task_id = 'dag_trigger_next_operator'
next_task_conf = next_task_id + '_conf'

def choose_branch(task_instance, dag_run):
    dag_conf = dag_run.conf
    task_list = dag_conf['task_list']
    next_task = task_list[0]
    later_tasks = task_list[1:]
    conf_list = dag_conf['conf_list']
    # dump to string because value is stringified into
    # template string, is then parsed.
    next_conf = json.dumps(conf_list[0])
    later_confs = conf_list[1:]

    task_instance.xcom_push(key=next_task_id, value=next_task)
    task_instance.xcom_push(key=next_task_conf, value=next_conf)

    if later_tasks:
        repeat_conf = json.dumps({
            'task_list': later_tasks,
            'conf_list': later_confs
        })

        task_instance.xcom_push(key=repeat_task_conf, value=repeat_conf)
        return [next_task_id, repeat_task_id]

    return next_task_id

def add_braces(in_string):
    return '{{' + in_string + '}}'

def make_templated_pull(key):
    pull = f'ti.xcom_pull(key=\'{key}\', task_ids=\'{branch_id}\')'
    return add_braces(pull)

with DAG(
    dag_id,
    start_date=days_ago(2),
    tags=['my_test'],
    default_args=DEFAULT_ARGS,
    description='A simple test DAG',
    schedule_interval=None,
) as dag:
    branch = BranchPythonOperator(
        task_id=branch_id,
        python_callable=choose_branch
    )

    trigger_next = TriggerDagRunOperator(
        task_id=next_task_id,
        trigger_dag_id=make_templated_pull(next_task_id),
        conf=make_templated_pull(next_task_conf)
    )

    trigger_repeat = TriggerDagRunOperator(
        task_id=repeat_task_id,
        trigger_dag_id=dag_id,
        conf=make_templated_pull(repeat_task_conf)
    )

    branch >> [trigger_next, trigger_repeat]


