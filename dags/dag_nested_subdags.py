#https://gist.github.com/semihsezer/87bdd6991415d883e37c8040f08d2238
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 10, 18)
}

# Python callable methods for python operator
def schedule_tasks_method(**kwargs):
    return 'Scheduling many tasks...'

def process_task(**kwargs):
    return 'Processing task...'

# Helper methods for subdag and subtask creation
def create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
  ''''Returns a DAG which has the dag_id formatted as parent.child '''
  return DAG(
    dag_id='{}.{}'.format(parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    start_date=start_date,
    default_args=default_args,
    max_active_runs=15
  )

def create_tasks(somedag, task_ids):
  '''Creates tasks for the given dag'''
  for task_id in task_ids:
      dummy_operator = PythonOperator(
        task_id='{}'.format(task_id),
        python_callable=process_task,
        dag=somedag,
  )

# ---------------------------------------------

# Top dag
dag_id = 'dag_nested_subdags'
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=15,
    tags=['loop','PythonOperator','SubDagOperator'],
)

# Top dag initial task
schedule_tasks = PythonOperator(
    task_id='schedule_tasks',
    provide_context=True,
    python_callable=schedule_tasks_method,
    dag=dag,
)

# Recursively create and assign subdags to parent dags.
# Remember, subdags are still DAGs. SubDagOperator only bundles it as a task for the parent dag.
level1_list = ['AWS', 'AZURE']
level2_list = ['eu', 'us', 'ap', 'jp']
tasks = ['task_{}'.format(str(i)) for i in range(0, 10)]
level1_subdag_operators = []

for level1_item in level1_list:
    level1_dag = create_sub_dag(dag_id, level1_item, datetime(2022, 10, 18), None)
    level1_subdag_operator = SubDagOperator(
      subdag=level1_dag,
      task_id=level1_item,
      dag=dag,
    )
    level1_subdag_operators.append(level1_subdag_operator)

    for level2_item in level2_list:
        level1_dag_id = '{}.{}'.format(dag_id, level1_item)
        level2_dag = create_sub_dag(level1_dag_id, level2_item, datetime(2022, 10, 18), None)
        level2_subdag_operator = SubDagOperator(
          subdag=level2_dag,
          task_id=level2_item,
          dag=level1_dag,
        )
        
        create_tasks(level2_dag, tasks)

    # Set dag objects to null so that they don't appear on Dags UI
    # Won't be needed if you move it to another file or scope
    level1_dag = None
    level2_dag = None

# This is optional
schedule_tasks >> level1_subdag_operators
