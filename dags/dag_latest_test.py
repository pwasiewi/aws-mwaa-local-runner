from datetime import datetime

from airflow import DAG
#from airflow.operators.empty import EmptyOperator airflow > 2.3.0
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='dag_latest_test',
    start_date=datetime(2022, 10, 1),
    catchup=True,
    schedule_interval="@daily",
    tags=['latest_only'],
) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    task1 = DummyOperator(task_id='task1')
    task2 = DummyOperator(task_id='task2')
    task3 = DummyOperator(task_id='task3')
    task4 = DummyOperator(task_id='task4', trigger_rule=TriggerRule.ALL_SUCCESS)

    latest_only >> task1 >> [task3, task4]
    task2 >> [task3, task4]
