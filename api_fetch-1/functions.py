
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import mysql.connector
from mysql.connector import Error

mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
engine = create_engine(mysql_connection_string)

def fetch_data(api_url, params, no_of_records=None):
    """
    Fetch data from EIA API

    :param api_url: Provide the base API URL.
    :param params: Provide parameters for the data, e.g., your API key, frequency.
    :param no_of_records: Maximum number of records to fetch (default is None, meaning fetch all records).
    :return: A pandas DataFrame containing the fetched data.
    """




    params['offset'] = 0
    # complete_data = pd.DataFrame()
    total_records_fetched = 0
    while True:
        response = requests.get(api_url, params=params)
      
        data = response.json()     #store the respone in json format in data
        records = data['response']['data']   #fetch the data from nested dictionary 
       
        if not records:
            break

        df = pd.DataFrame(records)
        # complete_data = pd.concat([complete_data, df], ignore_index=True)
        total_records_fetched += len(records)      
        params['offset'] += len(records)   #increase offset by the no. of records fetched in the loop
        df=df[['period','respondent-name','type-name','value','value-units']]
        mysql_connect(df,'final_run' )
        
        
    
    

    

def mysql_connect(dataframe, table_name):
    """
    Insert DataFrame into MySQL.
    :param dataframe: DataFrame name
    :param table_name: Table name
    """

    

    with engine.begin() as connection:
        dataframe.to_sql(table_name, con=connection, if_exists='append', index=False)

    print("stored in sql")


def call_stored_procedure():
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



