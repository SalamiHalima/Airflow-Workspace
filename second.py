from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.python_operator import PythonOperator

# first we write here our pyhton logic
 
 # defining a function
def python_first_function():
    print(str(datetime.now()))

#create the DAG which calls the python logic that you had created above.

default_dag_args = {
    'start_date': datetime(2023, 2, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1

}

# crontab notation can be useful https://crontab.guru/#0_0_*_*_1

with DAG('first_python_dag', schedule_interval= '@daily', catchup=False, default_args=default_dag_args) as dag_python:
    
    # here we define our tasks
    task_0 =PythonOperator(task_id = 'first_python_task', python_callable=python_first_function)
