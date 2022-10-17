from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain


with DAG(dag_id='dag_acyclic_01',
     start_date=datetime(2022, 10, 1),
     catchup=False,
     schedule_interval="@daily",
     tags=['dummy', 'flow'],
         ) as dag:

     t0 = DummyOperator(task_id='t0')
     t1 = DummyOperator(task_id='t1')
     t2 = DummyOperator(task_id='t2')
     t3 = DummyOperator(task_id='t3')
    
     ### Using set_downstream():
#     t0.set_downstream(t1)
#     t1.set_downstream(t2)
#     t2.set_downstream(t3)
     ### Using >>
     t0 >> t1 >> t2 >> t3
     ### Using set_upstream():
#     t3.set_upstream(t2)
#     t2.set_upstream(t1)
#     t1.set_upstream(t0)
     ### Using <<
#     t3 << t2 << t1 << t0
