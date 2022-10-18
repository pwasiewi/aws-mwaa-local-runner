import datetime as dt
import airflow.utils.dates
from ssh_plugin import SSHHook2
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.helpers import cross_downstream
from datetime import timedelta

layers = 3
ntasks = 1
sleep_seconds = 2

dag = DAG(dag_id= "dag_bash_sleep", 
          schedule_interval="@once", 
          start_date=dt.datetime(2022, 10, 19, 10, 0, 0), 
          is_paused_upon_creation=False,
          tags=['loop', 'BashOperator', 'sleep'])
previous_layer = None
for layer_n in range(layers):
    current_layer = [BashOperator(task_id=f"task{layer_n}-{task_n}", 
                                  retries=2, 
                                  retry_delay=timedelta(seconds=60), 
                                  dag=dag, 
                                  bash_command=f"sleep {sleep_seconds}") for task_n in range(ntasks)]
    if previous_layer:
        cross_downstream(previous_layer, current_layer)
    previous_layer = current_layer
