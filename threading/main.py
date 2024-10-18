# main.py

from functions import fetch_data, call_stored_procedure, failed_offset
from apicalls import api_calls

api_key = 'ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae'

mysql_credentials = {
    "username": 'root',
    "password": 'root',
    "host": '192.168.3.112',
    "database": 'eia'
}

if __name__ == '__main__':
    # Loop through the API calls
    for api_call in api_calls:
        # Fetch data from API and insert into MySQL
        fetch_data(api_call, api_key, mysql_credentials)
        # Call stored procedure after data insertion
        call_stored_procedure('calculate_co2_reduction', mysql_credentials)
        print(failed_offset)
