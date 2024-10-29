from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import pandas as pd
import requests
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from pydantic import BaseModel, constr, condecimal, ValidationError, conint



failed_offset = []
base_url = "https://api.eia.gov/v2/"
db_lock = threading.Lock()

def get_last_offset(table_name, mysql_credentials):
    """
    Fetch the last offset from MySQL.
    """
    try:
        connection = mysql.connector.connect(
            host=mysql_credentials['host'],
            database=mysql_credentials['database'],
            user=mysql_credentials['username'],
            password=mysql_credentials['password']
        )
        cursor = connection.cursor()
        cursor.execute(f"SELECT max(last_offset) FROM api_fetch_offsets WHERE table_name ={table_name}")
        result = cursor.fetchone()
        print(result,result[0])
        if result and result[0] is not None:
             return result[0]

        else:
            return 0
    except Error as e:
        print(f"Error fetching last offset: {str(e)}")
        return 0
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def update_last_offset(table_name, offset, mysql_credentials):
    """
    Update the last offset in MySQL.
    """
    try:
        connection = mysql.connector.connect(
            host=mysql_credentials['host'],
            database=mysql_credentials['database'],
            user=mysql_credentials['username'],
            password=mysql_credentials['password']
        )
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO api_fetch_offsets (table_name, last_offset) VALUES ({table_name}, {offset})")
        connection.commit()
        print(f"Updated last fetched offset for {table_name} to {offset}.")
    except Error as e:
        print(f"Error updating last offset: {str(e)}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def fetch_data(api_call, api_key, mysql_credentials):
    table_name = api_call['table_name']
    last_offset = get_last_offset(table_name, mysql_credentials)  # Get last fetched offset
    url = f"{base_url}{api_call['url']}"
    params = api_call['params']
    params['api_key'] = api_key

    try:
        print(f"Fetching total records for {table_name} with URL: {url} and params: {params}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        total_records = int(json_data['response']['total'])
        print(f"Total records available: {total_records}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            offset_map = {}
            for offset in range(last_offset, total_records, 5000):
                temp_params = params.copy()
                temp_params['offset'] = offset
                print(f"Submitting request for offset: {offset}")
                future = executor.submit(requests.get, url, temp_params)
                offset_map[future] = offset
                time.sleep(0.2)

            for future in as_completed(offset_map):
                offset = offset_map[future]
                try:
                    response = future.result()
                    response.raise_for_status()
                    data = response.json()

                    if 'data' in data['response']:
                        df = pd.DataFrame(data['response']['data'])
                        if df.empty:
                            print(f"No data found for offset {offset}.")
                        else:
                            print(f"Data fetched for offset {offset}: {len(df)} records.")
                            if 'columns' in api_call:
                                df = df[api_call['columns']]
                            df['value'] = pd.to_numeric(df['value'], errors='coerce')

                           

                            
                            update_last_offset(table_name, offset, mysql_credentials)

                except requests.exceptions.HTTPError as e:
                    print(f"Error fetching data at offset {offset}: {str(e)}")
                    failed_offset.append(offset)

    except Exception as e:
        print(f"An error occurred while fetching data for {table_name}: {str(e)}")
        failed_offset.append(offset)


def mysql_connect(df, table_name, mysql_credentials, offset, dtype):
    engine = create_engine(f"mysql+pymysql://{mysql_credentials['username']}:{mysql_credentials['password']}@{mysql_credentials['host']}/{mysql_credentials['database']}")
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Inserted {len(df)} records into {table_name} at offset {offset}.")
    except Exception as e:
        print(f"Error inserting into {table_name}: {str(e)}")


