# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import functions as f
# from airflow.models import Variable
# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 10, 15),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Create the DAG
# dag = DAG(
#     'eia_data_ingestion',
#     default_args=default_args,
#     description='Fetch and store EIA data in MySQL',
#     schedule_interval='@daily',  # You can change this to your preferred schedule
# )

# # MySQL credentials
# mysql_credentials = {
#     "username": 'root',
#     "password": 'root',
#     "host": '192.168.3.112',
#     "database": 'eia'
# }

# # API calls configuration
# api_calls = [
#     {
#         "base_url": "https://api.eia.gov/v2/",
#         "url": "co2-emissions/co2-emissions-aggregates/data/",
#         "params": {
#             "frequency": "annual",
#             "data[0]": "value"
#         },
#         "table_name": "emission_co2_source"
#     },
#     {
#         "base_url": "https://api.eia.gov/v2/",
#         "url": "international/data/",
#         "params": {
#             "frequency": "annual",
#             "data[0]": "value",
#             "facets[productId][]": [116, 33, 37],
#             "facets[countryRegionId][]": "USA"
#         },
#         "table_name": "renewable_generation_source",
#         "filter": lambda df: df[((df['activityName'] == 'Generation') & (df['unitName'] == 'billion kilowatthours')) | (df['activityName'] == 'Capacity')]
#     },
#     {
#         "base_url": "https://api.eia.gov/v2/",
#         "url": "electricity/rto/daily-fuel-type-data/data/",
#         "params": {
#             "frequency": "daily",
#             "data[0]": "value"
#         },
#         "table_name": "new_daily_data",
#         'no_of _records': 2000
#     }
# ]

# # Define the task to process API calls
# fetch_and_store_data_task = PythonOperator(
#     task_id='fetch_and_store_data',
#     python_callable=f.process_api_calls,
#     op_kwargs={
#         'api_calls': api_calls,
#         'mysql_credentials': mysql_credentials,
#         'api_key': Variable.get('api_key'),
#     },
#     dag=dag,
# )

# # Define the task to call the stored procedure
# call_stored_procedure_task = PythonOperator(
#     task_id='call_stored_procedure',
#     python_callable=f.call_stored_procedure,
#     op_kwargs={
#         'proc_name': 'calculate_co2_reduction',
#         'mysql_credentials': mysql_credentials,
#     },
#     dag=dag,
# )

# # Set the task dependencies
# fetch_and_store_data_task >> call_stored_procedure_task
