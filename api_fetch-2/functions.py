
import requests
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error

def fetch_data(base_url, url, params, api_key, no_of_records=None):
    """
    Fetch data from EIA API.
    :param base_url: Base URL for the API.
    :param url: API url (specific URL path).
    :param params: Parameters for the API call.
    :param api_key: Your API key.
    :param no_of_records: Maximum number of records to fetch.
    :return: DataFrame containing the fetched data.
    """
    complete_data = pd.DataFrame()
    params['api_key'] = api_key
    params['offset'] = 0

    while True:
        response = requests.get(f"{base_url}{url}", params=params)
        response.raise_for_status()  # Raise an error for bad responses
        records = response.json().get('response', {}).get('data', [])

        if not records:
            break

        df = pd.DataFrame(records)
        complete_data = pd.concat([complete_data, df], ignore_index=True)
        params['offset'] += len(records)

        if no_of_records is not None and len(complete_data) >= no_of_records:
            return complete_data.iloc[:no_of_records]
        print(len(complete_data))
    
    return complete_data


def insert_data_to_mysql(dataframe, table_name, username, password, host, database):
    """
    Insert DataFrame into MySQL.
    :param dataframe: DataFrame to insert.
    :param table_name: Target table name.
    :param username: MySQL username.
    :param password: MySQL password.
    :param host: MySQL host.
    :param database: MySQL database name.
    """
    connection_string = f'mysql+pymysql://{username}:{password}@{host}/{database}'
    engine = create_engine(connection_string)

    with engine.begin() as connection:
        dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)
    
    print(f"Data stored in table '{table_name}'.")


def call_stored_procedure(proc_name, username, password, host, database):
    """
    Execute a stored procedure in MySQL.
    :param proc_name: Name of the stored procedure to call.
    :param username: MySQL username.
    :param password: MySQL password.
    :param host: MySQL host.
    :param database: MySQL database name.
    """
    try:
        with mysql.connector.connect(
            host=host,
            database=database,
            user=username,
            password=password
        ) as connection:
            cursor = connection.cursor()
            cursor.callproc(proc_name)
            connection.commit()
            print(f"Stored procedure '{proc_name}' executed successfully.")
    except Error as e:
        print(f"Error: {e}")