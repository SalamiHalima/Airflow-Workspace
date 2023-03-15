from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# operator is airflow's way of talking to some of the language
# python operator, postgres operator, bashoperator.
#bashoperator allows us to write shell code.

default_dag_args = {
    'start_date': datetime(2023, 2, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}
# let's def a DAG
 
with DAG("First_DAG", schedule_interval=None, default_args= default_dag_args) as dag:

    # here at this level we def our tasks of the DAG
    task_0= BashOperator(task_id= 'bash_task', bash_command = "echo 'command executed from Bash Operator'")
    task_1= BashOperator(task_id= 'bash_task_move', bash_command = "cp C:\\Users\\pc1\\develhope_airf\\DATA_CENTER\\DATA_LAKE\\datast_raw.txt C:\\Users\\pc1\\develhope_airf\\DATA_CENTER\\CLEAN_DATA\\datast_raw.txt")

# IN the end of your DAG definition, we want to write the dependencies btw tasks. >> <<

task_0 >> task_1