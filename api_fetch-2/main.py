import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import secret
import time  # Import time module for sleep

def fetch_total_records(api_details, api_key):
    """
    Fetch the total number of records available from the API.
    :param api_details: Dictionary with API information (base URL, endpoint, params).
    :param api_key: Your API key.
    :return: Total number of records.
    """
    base_url = api_details['base_url']
    url = api_details['url']
    params = api_details['params']
    params['api_key'] = api_key
    params['offset'] = 0  # Start with the first record
    params['length'] = 1  # Fetch just one record to get the total number of records

    response = requests.get(f"{base_url}{url}", params=params)
    response.raise_for_status()
    total_records = int(response.json().get('response', {}).get('total', 0))
    print(f"Total records available: {total_records}")
    return total_records

def fetch_data(api_details, api_key, chunk_size):
    """
    Fetch data from the API in parallel using ThreadPoolExecutor.
    :param api_details: Dictionary with API information (base URL, endpoint, params).
    :param api_key: Your API key.
    :param chunk_size: Number of records to fetch in each request.
    :return: Combined DataFrame containing all fetched data.
    """
    complete_data = pd.DataFrame()
    error_offsets = []  # List to store offsets where errors occur
    total_records = fetch_total_records(api_details, api_key)

    # Start fetching data in chunks
    offsets = list(range(0, total_records, chunk_size))

    with ThreadPoolExecutor(max_workers=8) as executor:  # Use 5 workers
        future_to_offset = {}
        
        for offset in offsets:
            # Prepare the request parameters for each offset
            params = api_details['params'].copy()  # Make a copy to avoid overwriting
            params['api_key'] = api_key
            params['offset'] = offset
            params['length'] = chunk_size
            
            # Submit each offset as a task to the thread pool
            future = executor.submit(requests.get, f"{api_details['base_url']}{api_details['url']}", params)
            future_to_offset[future] = offset
        
        # Collect results and append data to the complete DataFrame
        for future in as_completed(future_to_offset):
            offset = future_to_offset[future]
            try:
                response = future.result()
                response.raise_for_status()  # Check for HTTP errors
                records = response.json().get('response', {}).get('data', [])

                complete_data = pd.concat([complete_data, pd.DataFrame(records)], ignore_index=True)
                print(f"Fetched {len(records)} records from offset {offset}.total records are {len(complete_data)}")
            except Exception as e:
                print(f"Error fetching data from offset {offset}: {e}")
                error_offsets.append(offset)  # Store the offset where the error occurred
                time.sleep(10)  # Sleep for 3 seconds on error

    return complete_data, error_offsets  # Return both data and error offsets

if __name__ == "__main__":
    # Configuration
    base_url = "https://api.eia.gov/v2/"
    api_key = secret.api_key

    # API call configuration
    api_call = {
        "base_url": base_url,
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        }
    }

    # Execute API calls in chunks using threads and store all the data in a DataFrame
    chunk_size = 5000  # Change this to the maximum supported by the API if possible
    all_data, error_offsets = fetch_data(api_call, api_key, chunk_size)

    # Print the total number of records fetched
    print(f"Total records fetched: {len(all_data)}")
    
    # Print the offsets where errors occurred
    if error_offsets:
        print(f"Errors occurred at offsets: {error_offsets}")
    else:
        print("No errors occurred during data fetching.")
    
    # Save to CSV or perform other operations if needed
    all_data.to_csv('fetched_data.csv', index=False)
    print("Data saved to 'fetched_data.csv'.")
