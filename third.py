import requests
import time
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

# to make your functions accept params, you must define the key word args -> kwargs

# by writing kwargs your airflow task is expecting a dict or args

def get_data(**kwargs):

    ticker = kwargs['ticker']
    api_key = "CFL86IRJ570HA9DN"
    # replace the 'demo' apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + ticker + '&apikey=' + api_key
    r = requests.get(url)

    try:
        data = r.json()
        path = ("/opt/airflow/data/DATA_CENTER/DATA_LAKE/")  
        with open(path + "stock_market_raw_data" + ticker + '_' + str(time.time()), "w") as outfile:
            json.dump(data, outfile)
    except:
        pass

def test_data_first_option(**kwargs):
    read_path = ("/opt/airflow/data/DATA_CENTER/DATA_LAKE/")
    ticker = kwargs['ticker']
    latest = np.max([float(file.split('_')[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]
    
    file = open(read_path + latest_file)
    data = json.load(file)

# write down some conds and then make a testing condition
    condition_1 = len(data.keys()) == 2
    condition_2 = 'Time Series (Daily)' in data.keys()
    condition_3 = 'Meta Data' in data.keys()

    if condition_1 and condition_2 and condition_3:
      pass
    else: 
        raise Exception('The data integrity has been compromised')

# Branch operator which test the cond in code and when it true it call one task or fail calls another task.
def test_data(**kwargs):
    read_path = ("/opt/airflow/data/DATA_CENTER/DATA_LAKE/")
    ticker = kwargs['ticker']
    latest = np.max([float(file.split('_')[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]
    
    file = open(read_path + latest_file)
    data = json.load(file)

# write down some conds and then make a testing condition
    condition_1 = len(data.keys()) == 2
    condition_2 = 'Time Series (Daily)' in data.keys()
    condition_3 = 'Meta Data' in data.keys()

    if condition_1 and condition_2 and condition_3:
        # For the branch op we want to return the name of another task
        return 'clean_market_data'
    else: 
        return 'failed_task_data'

def clean_data(**kwargs):
    read_path = ("/opt/airflow/data/DATA_CENTER/DATA_LAKE/")
    # get a way to read the most recent file from data
    ticker = kwargs['ticker']
    latest = np.max([float(file.split('_')[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]
    
    output_path = ("/opt/airflow/data/DATA_CENTER/CLEAN_DATA/")
    
    # loading the raw data from the file
    file = open(read_path + latest_file)
    data = json.load(file)

    clean_data = pd.DataFrame(data['Time Series (Daily)']).T
    clean_data['ticker'] = data['Meta Data']['2. Symbol']
    clean_data['meta_data'] = str(data['Meta Data'])
    clean_data['timestamp'] = pd.to_datetime('now')

  # we wanna store the result in a clean data lake.
    clean_data.to_csv(output_path + ticker + "_snapshot_daily" + str(pd.to_datetime('now')) + '.csv')
#create the DAG which calls the python logic that you had created above.

default_dag_args = {
    'start_date': datetime(2023, 2, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1

}

# crontab notation can be useful https://crontab.guru/#0_0_*_*_1

with DAG("market_data_alphavantage_dag", schedule_interval= '@daily', catchup=False, default_args=default_dag_args) as dag_python:
    
    # here we define our tasks
    task_0 =PythonOperator(task_id = "get_market_data", python_callable=get_data, op_kwargs= {'ticker': "IBM"})
    task_1 =BranchPythonOperator(task_id = "test_market_data", python_callable=test_data, op_kwargs= {'ticker': "IBM"})
    
    task_2_1 =PythonOperator(task_id = "clean_market_data", python_callable=clean_data, op_kwargs= {'ticker': "IBM"})
    task_2_2 = DummyOperator(task_id = 'failed_task_data')
    
    # example of moving data to a db (just a dummy)
    task_3 = DummyOperator(task_id = 'aggregate_all_data_to_db')
    
    task_0 >> task_1 
    task_1 >> task_2_1 >> task_3
    task_1 >> task_2_2 >> task_3