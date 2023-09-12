from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def task1():
    print("Executing task 1")

def task2():
    print("Executing task 2")

def task3():
    print("Executing task 3")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 5),
}

with DAG('sample_dag', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    task1 = PythonOperator(
        task_id='task1',
        python_callable=task1
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=task2
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=task3
    )

    end = DummyOperator(task_id='end')

    start >> task1 >> task2 >> task3 >> end
