# https://github.com/pwasiewi/eulinks/tree/master/airflowdbt
"""Example DAG demonstrating the usage of the BranchPythonOperator."""

import random
import logging
from datetime import datetime, date

from airflow import DAG
from airflow import settings
from airflow.utils.state import State
from airflow.models import TaskInstance
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator


def print_running_tasks():
    session = settings.Session()
    for task in session.query(TaskInstance) \
            .filter(TaskInstance.state == State.RUNNING) \
            .all():
        logging.info(f'task_id: {task.task_id}, dag_id: {task.dag_id}, start_date: {task.start_date}, '
                     f'hostname: {task.hostname}, unixname: {task.unixname}, job_id: {task.job_id}, pid: {task.pid}')

with DAG(
    dag_id='dag_branch_02',
    start_date=datetime(2022, 10, 1),
    catchup=True,
    schedule_interval="@daily",
    tags=['branch', 'NONE_FAILED_MIN_ONE_SUCCESS','loop','PythonOperator'],
) as dag:
    # run_this_first
    t0 = DummyOperator(
        task_id='t0',
    )

    options = ['t1', 't2', 't3', 't4']

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: random.choice(options),
    )
    
    t0 >> branching

    join = PythonOperator(
        task_id='join',
        python_callable=print_running_tasks,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    def option_execute(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'
    
    for option in options:
        generate_option = PythonOperator(
            task_id='{0}'.format(option),
            python_callable=option_execute,
            op_kwargs={'option': option, 'date': date}
        )
        
        follow_t = DummyOperator(
            task_id='follow_' + option,
        )

        branching >> Label('label_' + option) >> generate_option >> follow_t >> join
