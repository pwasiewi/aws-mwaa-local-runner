# https://airflow.apache.org/docs/apache-airflow/2.2.2/concepts/dags.html
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain


with DAG(dag_id='dag_acyclic_04',
    start_date=datetime(2022, 10, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=['dummy', 'example', 'dependencies','chain'],
         ) as dag:

    t0 = DummyOperator(task_id='t0')
    t1 = DummyOperator(task_id='t1')
    t2 = DummyOperator(task_id='t2')
    t3 = DummyOperator(task_id='t3')
    t4 = DummyOperator(task_id='t4')
    t5 = DummyOperator(task_id='t5')
    t6 = DummyOperator(task_id='t6')
    t7 = DummyOperator(task_id='t7')

    chain(t0, (t1, t2, t3), (t4, t5, t6), t7)
    
#    t0 >> t1 >> [t2,t3]
#    t2 >> t4
#    t3 >> t5
#    [t4, t5] >> t6
    
    
