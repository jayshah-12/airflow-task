
import requests
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error

def fetch_data(api_details, api_key, no_of_records=None):
    """
    Fetch data from an API.
    :param api_details: Dictionary with API information (base URL, endpoint, params).
    :param api_key: Your API key.
    :param no_of_records: Maximum number of records to fetch.
    :return: DataFrame containing the fetched data.
    """
    base_url = api_details['base_url']
    url = api_details['url']
    params = api_details['params']
    complete_data = pd.DataFrame()
    params['api_key'] = api_key
    params['offset'] = 0

    while True:
        response = requests.get(f"{base_url}{url}", params=params)
        response.raise_for_status()
        records = response.json().get('response', {}).get('data', [])
        
        if not records:
            break

        df = pd.DataFrame(records)
        complete_data = pd.concat([complete_data, df], ignore_index=True)
        params['offset'] += len(records)

        if no_of_records is not None and len(complete_data) >= no_of_records:
            return complete_data.iloc[:no_of_records]
        print(f"Fetched {len(complete_data)} records.")
    
    return complete_data

def insert_data_to_mysql(dataframe, table_name, mysql_credentials):
    """
    Insert DataFrame into MySQL.
    :param dataframe: DataFrame to insert.
    :param table_name: Target table name.
    :param mysql_credentials: Dictionary with MySQL connection details.
    """
    connection_string = f"mysql+pymysql://{mysql_credentials['username']}:{mysql_credentials['password']}@{mysql_credentials['host']}/{mysql_credentials['database']}"
    engine = create_engine(connection_string)

    with engine.begin() as connection:
        dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)
    
    print(f"Data stored in table '{table_name}'.")

def call_stored_procedure(proc_name, mysql_credentials):
    """
    Execute a stored procedure in MySQL.
    :param proc_name: Name of the stored procedure to call.
    :param mysql_credentials: Dictionary with MySQL connection details.
    """
    try:
        with mysql.connector.connect(
            host=mysql_credentials['host'],
            database=mysql_credentials['database'],
            user=mysql_credentials['username'],
            password=mysql_credentials['password']
        ) as connection:
            cursor = connection.cursor()
            cursor.callproc(proc_name)
            connection.commit()
            print(f"Stored procedure '{proc_name}' executed successfully.")
    except Error as e:
        print(f"Error: {e}")

def process_api_calls(api_calls, mysql_credentials, api_key):
    """
    Process the apis to call the functions
    :param api_calls: List of dictionaries containing API call details.
    :param mysql_credentials: Dictionary with MySQL connection details.
    :param api_key: API key to access the service.
    """
    for call in api_calls:
        data = fetch_data(call, api_key, no_of_records=call.get("no_of_records"))
        

        if 'filter' in call:
            data = call['filter'](data)
        if 'columns' in call:
            data = data[call['columns']]
        insert_data_to_mysql(data, call["table_name"], mysql_credentials)
