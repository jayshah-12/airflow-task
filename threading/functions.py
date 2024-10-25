# functions.py

import requests
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import mysql.connector
from mysql.connector import Error

failed_offset=[]
base_url = "https://api.eia.gov/v2/"
db_lock = threading.Lock()

def fetch_data(api_call, api_key, mysql_credentials):
    url = f"{base_url}{api_call['url']}"
    params = api_call['params']
    params['api_key'] = api_key  # Include the API key in the parameters
    dtype = api_call['dtype']
    try:
        # Fetch total records to set the range of offsets
        print(f"Fetching total records for {api_call['table_name']} with URL: {url} and params: {params}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        total_records = int(json_data['response']['total'])
        print(f"Total records available: {total_records}")

      
 
        # Thread executor to fetch data
        with ThreadPoolExecutor(max_workers=2) as executor:
            offset_map = {}      #map the offset with future object result
            for offset in range(0, total_records, 5000):
                # temp params to avoid offset change in the main params
                temp_params = params.copy()
                temp_params['offset'] = offset  # Set the specific offset for this request

                print(f"Submitting request for offset: {offset}")  # Debug statement
                future = executor.submit(requests.get, url, temp_params)  # Use the local copy
                offset_map[future] = offset
                # print(offset_map)

            for future in as_completed(offset_map): 
                offset = offset_map[future]
                try:
                    response = future.result()
                    print(response)
                    response.raise_for_status()  # Check for HTTP errors
                    data = response.json()

                    if 'data' in data['response']:
                        df = pd.DataFrame(data['response']['data'])
                        if df.empty: 
                            print(f"No data found for offset {offset}.")
                        else:
                            print(f"Data fetched for offset {offset}: {len(df)} records.")

                            if 'columns' in api_call:
                                df = df[api_call['columns']]
                            # Insert data into MySQL
                            with db_lock:
                                mysql_connect(df,api_call['table_name'],mysql_credentials,offset,dtype)
                                # engine = create_engine(
                                #     f"mysql+pymysql://{mysql_credentials['username']}:{mysql_credentials['password']}@{mysql_credentials['host']}/{mysql_credentials['database']}")
                                # df.to_sql(api_call['table_name'], engine, if_exists='append', index=False, dtype=dtype)
                                # print(f"Inserted {len(df)} records into {api_call['table_name']} at offset {offset}.")
                                time.sleep(0.5)  # Sleep for rate limiting

                    else:
                        print(f"Unexpected response structure for offset {offset}: {data}")

                except requests.exceptions.HTTPError as e:
                    # Display error message for the specific offset
                    print(f"Error fetching data at offset {offset}: {str(e)}")
                    failed_offset.append(offset)

    
    except Exception as e:
        print(f"An error occurred while fetching data for {api_call['table_name']}: {str(e)}")
        failed_offset.append(offset)

def mysql_connect(df, table_name, mysql_credentials, offset,dtype):
    engine = create_engine(
        f"mysql+pymysql://{mysql_credentials['username']}:{mysql_credentials['password']}@{mysql_credentials['host']}/{mysql_credentials['database']}")
    df.to_sql(table_name, engine, if_exists='append', index=False,dtype=dtype)
    print(f"Inserted {len(df)} records into {table_name} at offset {offset}.")


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
