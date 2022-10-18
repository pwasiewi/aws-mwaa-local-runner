# https://github.com/pwasiewi/eulinks/tree/master/airflowdbt
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
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/profile"
DBT_DEFAULTS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

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

value_1 = "5"
value_2 = "20"
def push(**context):
    """Pushes an XCom without a specific target"""
    context['ti'].xcom_push(key='value from pusher 1', value=value_1)

def push_by_returning(**context):
    """Pushes an XCom without a specific target, just by returning it"""
    return value_2

def my_sub_dag(parent_dag_id):
    # Step 1 - define the default parameters for the DAG
    default_args = {
      'owner': 'airflow',
      'depends_on_past': False,
      'start_date': datetime(2022, 10, 18),
      'email': ['airflow@example.com'],
      'email_on_failure': False,
      'email_on_retry': False,
      'retries': 1,
      'retry_delay': timedelta(minutes=5),
      'snowflake_conn_id': 'snowflake',
    }
   
    #p_val=Variable.get('v_val') #Variable passed to registered method
    #p_val=15

    # Step 2 - Create a DAG object
    my_sub_dag = DAG(dag_id = parent_dag_id+ '.' + 'my_sub_dag',
            schedule_interval=None ,
            default_args=default_args
        )

    # Step 3 - Define the method to check the condition for branching
    def my_check_condition(**context):
      #ti = context['ti']
      #p_val = ti.xcom_pull(key='value from pusher 1', task_ids='push')
      ti = context['ti']
      p_val = ti.xcom_pull(task_ids='push_task',key='the_message', dag_id='dag_xcom_begin')
      print("received message: '%s'" % p_val)
      if int(p_val)>=15 :
        return 'greater_Than_equal_to_15'
      else:
        return 'less_Than_15'

    # Step 4 - Create a Branching task to Register the method in step 3 to the branching API
    checkTask = BranchPythonOperator(
      task_id='check_task',
      python_callable=my_check_condition, #Registered method
      provide_context=True,
      dag=my_sub_dag
    )

    # Step 5 - Create tasks
    greaterThan15 = BashOperator(
      task_id= 'greater_Than_equal_to_15',
      bash_command="echo value is greater than or equal to 15",
      dag=my_sub_dag
    )

    lessThan15 = BashOperator(
      task_id= 'less_Than_15',
      bash_command="echo value is less than 15",
      dag=my_sub_dag
    )

    finalTask = BashOperator(
      task_id= 'join_task',
      bash_command="echo This is a join",
      trigger_rule=TriggerRule.ONE_SUCCESS,
      dag=my_sub_dag
    )


    # Step 6 - Define the sequence of tasks.
    lessThan15.set_upstream(checkTask)
    greaterThan15.set_upstream(checkTask)
    finalTask.set_upstream([lessThan15, greaterThan15])

    # Step 7 - Return the DAG
    return my_sub_dag

dag = DAG(
  dag_id='dag_xcom_begin',
  default_args=DEFAULT_ARGS,
#  start_date=datetime(2022, 10, 18),
  schedule_interval=None,
  tags=['xcom','PythonOperator','push','pull','message','BashOperator']
)

def push_function(**context):
    msg='17'
    print("message to push: '%s'" % msg)
    task_instance = context['task_instance']
    task_instance.xcom_push(key="the_message", value=msg)

push_task = PythonOperator(
    task_id='push_task', 
    python_callable=push_function,
    provide_context=True,
    dag=dag)

def pull_function(**context):
    ti = context['ti']
    msg = ti.xcom_pull(task_ids='push_task',key='the_message')
    print("received message: '%s'" % msg)

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_function,
    provide_context=True,
    dag=dag)

sub_dag = SubDagOperator(
        subdag=my_sub_dag("dag_xcom_begin"),
        task_id='my_sub_dag',
        dag=dag
    )

push_task >> pull_task >> sub_dag

