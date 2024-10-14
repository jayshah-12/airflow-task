
import secret
import pandas as pd
import functions as f


def main():
    base_url = "https://api.eia.gov/v2/"
    api_key = secret.api_key

    # Define MySQL credentials
    mysql_credentials = {
        "username": 'root',
        "password": 'root',
        "host": 'localhost',
        "database": 'eia'
    }

    # Define API calls
    api_calls = [
        {
            "url": "co2-emissions/co2-emissions-aggregates/data/",
            "params": {
                "frequency": "annual",
                "data[0]": "value"
            },
            "columns": ['period', 'fuel-name', 'state-name', 'value', 'value-units'],
            "table_name": "emission_co2_source"
        },
       
        {
            "url": "international/data/",
            "params": {
                "frequency": "annual",
                "data[0]": "value",
                "facets[productId][]": [116, 33, 37],
                "facets[countryRegionId][]": "USA"
            },
            "columns": ['period', 'productName', 'activityName', 'unitName', 'value'],
            "table_name": "renewable_generation_source",
            "filter": lambda df: df[((df['activityName'] == 'Generation') & (df['unitName'] == 'billion kilowatthours')) | (df['activityName'] == 'Capacity')]
        }
    ]

    # Fetch data and store in database
    for call in api_calls:
        data = f.fetch_data(base_url, call["url"], call["params"], api_key, no_of_records=call.get("no_of_records"))
        if 'filter' in call:
            data = call['filter'](data)
        df = data[call["columns"]]
        f.insert_data_to_mysql(df, call["table_name"], **mysql_credentials)

    f.call_stored_procedure('calculate_co2_reduction', **mysql_credentials)


if __name__ == "__main__":
    main()