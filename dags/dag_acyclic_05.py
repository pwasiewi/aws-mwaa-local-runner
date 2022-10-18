# https://github.com/pwasiewi/eulinks/tree/master/airflowdbt
# https://airflow.apache.org/docs/apache-airflow/2.2.2/concepts/dags.html
from datetime import datetime

from airflow import DAG
#from airflow.operators.empty import EmptyOperator airflow > 2.3.0
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='dag_acyclic_05',
    start_date=datetime(2022, 10, 1),
    catchup=True,
    schedule_interval="@daily",
    tags=['dummy', 'example', 'dependencies','latest_only'],
) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    t1 = DummyOperator(task_id='t1')
    t2 = DummyOperator(task_id='t2')
    t3 = DummyOperator(task_id='t3')
    t4 = DummyOperator(task_id='t4', trigger_rule=TriggerRule.ALL_SUCCESS)

    latest_only >> t1 >> [t3, t4]
    t2 >> [t3, t4]
