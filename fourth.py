from datetime import  timedelta
import time
import json
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.dates import days_ago


default_args = {
    'owner': 'aleema',
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

create_query = """
DROP TABLE IF EXISTS public.stock_market_daily;
CREATE TABLE public.stock_market_daily (id INT NOT NULL, ticker varchar(250), price_open FLOAT);
"""

# create a logic that populate the table with some data

insert_data_query = """
INSERT INTO public.stock_market_daily (id, ticker, price_open) 
values (1, 'IBM', 100), (2, 'IBM', 200), (3, 'IBM', 300),
(4, 'TSLA', 400), (5, 'TSLA', 500), (6, 'TSLA', 600)

"""
create_grouped_table = """
DROP TABLE IF EXISTS ticker_aggregated_data;
CREATE TABLE IF NOT EXISTS ticker_aggregated_data AS
SELECT ticker, avg(price_open)
from stock_market_daily
GROUP BY ticker;
"""
dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date =  days_ago(1))

# here you define the tasks by calling the operator
# (sql = "point to a sql file path.sql")
create_table =  PostgresOperator(task_id = "creation_of_table", 
    sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_aleema_local")

insert_data = PostgresOperator(task_id = "insertion_of_data", 
    sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_aleema_local")

group_data = PostgresOperator(task_id = "creating_grouped_table", 
    sql = create_grouped_table, dag = dag_postgres, postgres_conn_id = "postgres_aleema_local")

create_table >> insert_data >> group_data