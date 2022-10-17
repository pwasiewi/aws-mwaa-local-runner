from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain


with DAG(dag_id='dag_acyclic_02',
    start_date=datetime(2022, 10, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=['dummy', 'flow'],
         ) as dag:

    t0 = DummyOperator(task_id='t0')
    t1 = DummyOperator(task_id='t1')
    t2 = DummyOperator(task_id='t2')
    t3 = DummyOperator(task_id='t3')
    t4 = DummyOperator(task_id='t4')
    t5 = DummyOperator(task_id='t5')
    t6 = DummyOperator(task_id='t6')

    chain(t0, t1, (t2, t3), (t4, t5), t6)
    
#    t0 >> t1 >> [t2,t3]
#    t2 >> t4
#    t3 >> t5
#    [t4, t5] >> t6
    
    
