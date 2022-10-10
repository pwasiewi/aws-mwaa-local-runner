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
# starting_day = "{{ dag_run.conf['starting_day'] if dag_run.conf.get('starting_day') else '2021-11-01' }}"
# end_day = "{{ dag_run.conf['end_day'] if dag_run.conf.get('end_day') else '2021-11-30' }}"
database="{{ dag_run.conf['database'] if dag_run.conf.get('database') else 'dwh_piotr' }}"
warehouse="{{ dag_run.conf['warehouse'] if dag_run.conf.get('warehouse') else 'sac_event_wh' }}"
schema = "{{ dag_run.conf['schema'] if dag_run.conf.get('schema') else 'raw' }}"
role = "{{ dag_run.conf['role'] if dag_run.conf.get('role') else 'sysadmin' }}"


PARENT_DAG_ID='dag_event_xcom'


def my_sub_dag(parent_dag_id, child_dag_id, parent_schedule_interval, parent_default_args):
    # Create a SUBDAG object
    my_sub_dag = DAG(
            dag_id='{}.{}'.format(parent_dag_id, child_dag_id),
            schedule_interval=parent_schedule_interval,
            default_args=parent_default_args,
        )

    # Define the method to check 
    def my_check(**context):
      ti = context['ti']
      counter = ti.xcom_pull(task_ids='param_push', key='counter', dag_id=parent_dag_id)
      print("\n counter =" + counter)


    check = PythonOperator(
      task_id='check_task',
      python_callable=my_check, 
      provide_context=True,
      dag=my_sub_dag
    )

    def truncate_wrapper_fun(**context):
      ti = context['ti']
      day = ti.xcom_pull(task_ids='param_push', key='starting_day', dag_id=parent_dag_id)
      print("DAY: " + day)
      """
      truncate_sac_event_raw_logs = SnowflakeOperator(
        task_id="truncate_sac_event_logs_{}".format(day),
        sql=f"truncate table raw.sac_event_raw;",
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        snowflake_conn_id='snowflake',
        dag=dag
      )
      truncate_sac_event_raw_logs.execute(dict())
      """

    truncate_wrapper = PythonOperator(
        task_id='test_task1',
        python_callable=truncate_wrapper_fun,
        provide_context=True,
        dag=my_sub_dag
    )

    

    check >> truncate_wrapper

    # Return the DAG
    return my_sub_dag


def range_of_dates(start_of_range: date, end_of_range: date) -> Sequence[date]:
        if start_of_range <= end_of_range:
            return [
                start_of_range + timedelta(days=x)
                for x in range(0, (end_of_range - start_of_range).days + 1)
            ]
        return [
            start_of_range - timedelta(days=x)
            for x in range(0, (start_of_range - end_of_range).days + 1)
        ]

def days_counter(starting_day, end_day):
    end_of_range = datetime.strptime(end_day, '%Y-%m-%d')
    start_of_range = datetime.strptime(starting_day, '%Y-%m-%d')
    return (end_of_range - start_of_range).days + 1 
    
def param_wrapper(starting_day, end_day, database, warehouse, schema, role, **context):
    counter = days_counter(starting_day, end_day)
    print("message to push: '%s'" % counter)
    task_instance = context['task_instance']
    task_instance.xcom_push(key="counter", value=str(counter))
    task_instance.xcom_push(key="starting_day", value=starting_day)
    task_instance.xcom_push(key="end_day", value=end_day)
    task_instance.xcom_push(key="database", value=database)
    task_instance.xcom_push(key="warehouse", value=warehouse)
    task_instance.xcom_push(key="schema", value=schema)
    task_instance.xcom_push(key="role", value=role)
    days = days_range(starting_day, end_day)
    str_range = [date_obj.strftime('%Y-%m-%d') for date_obj in days]
    task_instance.xcom_push(key="days", value=str_range)

def days_range(starting_day, end_day):
    start_of_range = datetime.strptime(starting_day, '%Y-%m-%d')
    end_of_range = datetime.strptime(end_day, '%Y-%m-%d')
    start_of_range = datetime.strptime(starting_day, '%Y-%m-%d')
    days = range_of_dates(start_of_range, end_of_range) 
    return days
    

with DAG(
  dag_id=PARENT_DAG_ID,
  default_args=DEFAULT_ARGS,
#  start_date=datetime(2019, 04, 21),
  schedule_interval=None,
  tags=['test'],
  #schedule_interval=timedelta(1)
) as dag:

  starting_day = Variable.get('starting_date', default_var='2021-12-09')
  # print(starting_date)
  end_day = Variable.get('end_date', default_var='2021-12-10')
  # print(end_date)

  param_push = PythonOperator(
    task_id='param_push',
    python_callable=param_wrapper,
    op_kwargs={"starting_day": starting_day, "end_day": end_day, "database": database, "warehouse": warehouse, "schema": schema, "role": role},
    provide_context=True,
    dag=dag)
 

  days = days_range(starting_day, end_day)
  str_range = [date_obj.strftime('%Y-%m-%d') for date_obj in days]
  chain_operators = []
  chain_operators.append(param_push)
  for day in days:
    day2str = day.strftime('%Y-%m-%d')
    day2dag = day.strftime('%Y_%m_%d')
    event_sub_dag = 'sub_' + day2dag
    sub_dag = SubDagOperator(
        subdag=my_sub_dag(dag.dag_id, event_sub_dag, dag.schedule_interval, dag.default_args),
        task_id=event_sub_dag,
        dag=dag
    )
    chain_operators.append(sub_dag)


  # Add downstream
  for i,val in enumerate(chain_operators[:-1]):
        val.set_downstream(chain_operators[i+1])

