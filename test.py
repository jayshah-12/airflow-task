import requests
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

NUM_FETCH_WORKERS = 10  # Number of workers for fetching data
NUM_INSERT_WORKERS = 5   # Number of workers for inserting data
CHUNK_SIZE = 5000  # Number of records per API request


def fetch_worker(api_url, task_params):
    """
    Worker function to fetch a portion of data from the API.
    """
    try:
        response = requests.get(api_url, params=task_params)
        response.raise_for_status()
        data = response.json()
        records = data['response']['data']
        return pd.DataFrame(records)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def fetch_data_concurrently(api_url, params, total_records, chunk_size=5000):
    """
    Fetch data concurrently from an API in chunks.
    :param api_url: The base URL for the API.
    :param params: API parameters (including API key).
    :param total_records: Total number of records to fetch.
    :param chunk_size: Number of records per request.
    :return: Combined DataFrame with all the fetched data.
    """
    offset = 0
    complete_data = pd.DataFrame()

    with ThreadPoolExecutor(max_workers=NUM_FETCH_WORKERS) as executor:
        futures = []

        while offset < total_records:
            task_params = params.copy()
            task_params['offset'] = offset
            task_params['length'] = chunk_size
            futures.append(executor.submit(fetch_worker, api_url, task_params))
            offset += chunk_size

        for future in as_completed(futures):
            df = future.result()
            complete_data = pd.concat([complete_data, df], ignore_index=True)

    return complete_data


def insert_worker(data_chunk, table_name, mysql_connection_string):
    """
    Worker function to insert a chunk of data into MySQL.
    """
    engine = create_engine(mysql_connection_string)
    with engine.begin() as connection:
        data_chunk.to_sql(table_name, con=connection, if_exists='append', index=False)
    print(f"Inserted {len(data_chunk)} rows into {table_name}")


def batch_mysql_insert_concurrently(dataframe, table_name, chunk_size=10000):
    """
    Insert DataFrame into MySQL in parallel batches using ThreadPoolExecutor.
    :param dataframe: DataFrame to insert into MySQL.
    :param table_name: MySQL table name.
    :param chunk_size: Number of rows per insert batch.
    """
    mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'

    with ThreadPoolExecutor(max_workers=NUM_INSERT_WORKERS) as executor:
        futures = []

        # Split DataFrame into chunks and insert concurrently
        for i in range(0, len(dataframe), chunk_size):
            chunk = dataframe.iloc[i:i + chunk_size]
            futures.append(executor.submit(insert_worker, chunk, table_name, mysql_connection_string))

        # Ensure all insertions are completed
        for future in as_completed(futures):
            future.result()


def call_stored_procedure():
    import mysql.connector
    from mysql.connector import Error

    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='eia',
            user='root',
            password='root'
        )

        if connection.is_connected():
            cursor = connection.cursor()
            cursor.callproc('calculate_co2_reduction')
            connection.commit()
            print("Stored procedure executed successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


# Example usage
base_url = "https://api.eia.gov/v2/"
api2_url = f"{base_url}electricity/rto/daily-fuel-type-data/data/"

api2_params = {
    "frequency": "daily",
    "data[0]": "value",
    "api_key": "ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"
}

# Total records to fetch (adjust based on the API's maximum limit)
total_records = 4000000

# Fetch data concurrently in chunks
start_time = time.time()
data_api2 = fetch_data_concurrently(api2_url, api2_params, total_records, chunk_size=CHUNK_SIZE)
print(f"Data fetching completed in {time.time() - start_time:.2f} seconds")

# Insert data into MySQL in parallel batches
start_time = time.time()
batch_mysql_insert_concurrently(data_api2, "daily_electricity_source")
print(f"Data insertion completed in {time.time() - start_time:.2f} seconds")

# Call the stored procedure
call_stored_procedure()
