import requests
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import mysql.connector
from mysql.connector import Error

# Global Variables
failed_offset = []
base_url = "https://api.eia.gov/v2/"
db_lock = threading.Lock()

api_key = 'ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae'

mysql_credentials = {
    "username": 'root',
    "password": 'root',
    "host": '192.168.3.112',
    "database": 'eia'
}


api_calls = [
    {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "new_data2"
    },
]


def fetch_data(api_call, api_key, mysql_credentials):
    url = f"{base_url}{api_call['url']}"
    params = api_call['params']
    params['api_key'] = api_key  # Include the API key in the parameters

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        total_records = int(json_data['response']['total'])
        print(f"Total records are {total_records}")

        # Calculate the batch size: Total records divided by 5
        large_batch_size = total_records // 5
        small_batch_size = 5000  # Use 5,000 as the smaller batch size for each request
        
        # Create large batch offsets, dividing total records into 5 parts
        large_batch_offsets = range(0, total_records, large_batch_size)

        # Process each larger batch sequentially
        for large_batch_start in large_batch_offsets:
            print(f"Processing batch from {large_batch_start} to {large_batch_start + large_batch_size}")

            # Create smaller offsets within the current large batch (fetching 5,000 records at a time)
            small_batch_offsets = range(large_batch_start, min(large_batch_start + large_batch_size, total_records), small_batch_size)

            # Thread executor to fetch data for each smaller offset within the current large batch
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_offset = {}
                for offset in small_batch_offsets:
                    params['offset'] = offset
                    future = executor.submit(requests.get, url, params)
                    future_to_offset[future] = offset

                # Collect results from all workers in the current large batch
                for future in as_completed(future_to_offset):
                    offset = future_to_offset[future]
                    try:
                        response = future.result()
                        response.raise_for_status()  # Check for HTTP errors
                        data = response.json()

                        if 'data' in data['response']:
                            # Create DataFrame from the API response
                            df = pd.DataFrame(data['response']['data'])

                            # Check if the DataFrame contains expected columns
                            if not df.empty:
                                print(f"Data fetched for offset {offset}: {len(df)} records.")

                                # Select only the specified columns and reorder
                                try:
                                    df = df[api_call['columns']]
                                except KeyError as e:
                                    print(f"KeyError: {e}. Available columns in DataFrame: {df.columns.tolist()}")

                                # Insert data into MySQL
                                with db_lock:
                                    mysql_connect(df, api_call['table_name'], mysql_credentials, offset)
                                    time.sleep(0.5)  # Sleep for rate limiting

                        else:
                            print(f"Unexpected response structure for offset {offset}: {data}")

                    except Exception as e:
                        print(f"General error occurred at offset {offset}: {str(e)}")
                        failed_offset.append(offset)

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching total records for {api_call['table_name']}: {str(e)}")
    except Exception as e:
        print(f"An error occurred while fetching data for {api_call['table_name']}: {str(e)}")


# Function to connect to MySQL and insert data
def mysql_connect(df, table_name, mysql_credentials, offset):
    engine = create_engine(
        f"mysql+pymysql://{mysql_credentials['username']}:{mysql_credentials['password']}@{mysql_credentials['host']}/{mysql_credentials['database']}")
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} records into {table_name} at offset {offset}.")


# Function to call stored procedures in MySQL
def call_stored_procedure(proc_name, mysql_credentials):
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


# Main function to execute API calls and process data
if __name__ == '__main__':
    # Loop through the API calls
    for api_call in api_calls:
        # Fetch data from API and insert into MySQL
        fetch_data(api_call, api_key, mysql_credentials)
        # Call stored procedure after data insertion
        call_stored_procedure('calculate_co2_reduction', mysql_credentials)
        print(failed_offset)
