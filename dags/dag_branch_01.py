# https://github.com/pwasiewi/eulinks/tree/master/airflowdbt
"""Example DAG demonstrating the usage of the BranchPythonOperator."""

import random
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='dag_branch_01',
    start_date=datetime(2022, 10, 1),
    catchup=True,
    schedule_interval="@daily",
    tags=['branch', 'NONE_FAILED_MIN_ONE_SUCCESS','loop'],
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

    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for option in options:
        t = DummyOperator(
            task_id=option,
        )

        t_follow = DummyOperator(
            task_id='follow_' + option,
        )

        # Label is optional here, but it can help identify more complex branches
        branching >> Label('label_' + option) >> t >> t_follow >> join
