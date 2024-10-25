from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from functions import fetch_data, call_stored_procedure
from apicalls import api_calls
from airflow.models import Variable
from datetime import datetime
api_key = Variable.get('api_key')

mysql_credentials = {
    "username": 'root',
    "password": 'root',
    "host": '192.168.3.112',
    "database": 'eia'
}

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 15),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'eia_threading',
    default_args=default_args,
    description='DAG to fetch EIA API data and insert into MySQL',
    schedule_interval='@daily', 
    catchup= False, 
)

# Task 1: Fetch data for all API calls
def fetch_eia_data(**kwargs):
    for api_call in api_calls:
        fetch_data(api_call, api_key, mysql_credentials)

# Task 2: Call stored procedure after data is inserted
def call_procedure(**kwargs):
    call_stored_procedure('calculate_co2_reduction', mysql_credentials)

# PythonOperator to fetch data
fetch_data_task = PythonOperator(
    task_id='fetch_eia_data',
    python_callable=fetch_eia_data,
    provide_context=True,
    dag=dag,
)

# PythonOperator to call the stored procedure
call_procedure_task = PythonOperator(
    task_id='call_stored_procedure',
    python_callable=call_procedure,
    provide_context=True,
    dag=dag,
)

# Task dependencies
fetch_data_task >> call_procedure_task
