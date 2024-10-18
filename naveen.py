import requests
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time



# Configuration
base_url = "https://api.eia.gov/v2/"
api_key = 'ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae'
failed_offset=[]
# MySQL credentials
mysql_credentials = {
    "username":'root',
    "password": 'root',
    "host": '192.168.3.112',
    "database": 'eia'
}

# API calls configuration
api_calls = [
    {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "jayshah" 
    },
]

# Thread lock for database operations
db_lock = threading.Lock()
def fetch_data(api_call, api_key, mysql_credentials):
    url = f"{base_url}{api_call['url']}"
    params = api_call['params']
    params['api_key'] = api_key  # Include the API key in the parameters

    try:
        # Fetch total records to determine offsets
        print(f"Fetching total records for {api_call['table_name']} with URL: {url} and params: {params}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        total_records = int(json_data['response']['total'])
        print(f"Total records available for {api_call['table_name']}: {total_records}")

        offsets = range(0, total_records, 5000)

        # Thread executor to fetch data
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_offset = {}
            for offset in offsets:
                # Create a local copy of params for this offset
                local_params = params.copy()
                local_params['offset'] = offset  # Set the specific offset for this request

                print(f"Submitting request for offset: {offset}")  # Debug statement
                future = executor.submit(requests.get, url, local_params)  # Use the local copy
                future_to_offset[future] = offset

            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                try:
                    response = future.result()
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
                                engine = create_engine(
                                    f"mysql+pymysql://{mysql_credentials['username']}:{mysql_credentials['password']}@{mysql_credentials['host']}/{mysql_credentials['database']}")
                                df.to_sql(api_call['table_name'], engine, if_exists='append', index=False)
                                print(f"Inserted {len(df)} records into {api_call['table_name']} at offset {offset}.")
                                time.sleep(0.5)  # Sleep for rate limiting

                    else:
                        print(f"Unexpected response structure for offset {offset}: {data}")

                except requests.exceptions.HTTPError as e:
                    # Display error message for the specific offset
                    print(f"Error fetching data at offset {offset}: {str(e)}")
                    failed_offset.append(offset)

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching total records for {api_call['table_name']}: {str(e)}")
    except Exception as e:
        print(f"An error occurred while fetching data for {api_call['table_name']}: {str(e)}")



for api_call in api_calls:
    fetch_data(api_call, api_key, mysql_credentials)
