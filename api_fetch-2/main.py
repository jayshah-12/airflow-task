
import secret
import functions as f

if __name__ == "__main__":
    # Configuration
    base_url = "https://api.eia.gov/v2/"
    api_key = secret.api_key

    # MySQL credentials
    mysql_credentials = {
        "username": secret.sql_user,
        "password": secret.sql_pass,
        "host": '192.168.3.112',
        "database": 'eia'
    }

    # API calls configuration
    api_calls = [
        {
            "base_url": base_url,
            "url": "co2-emissions/co2-emissions-aggregates/data/",
            "params": {
                "frequency": "annual",
                "data[0]": "value"
            },
            "columns": ['period', 'fuel-name', 'state-name', 'value', 'value-units'],
            "table_name": "emission_co2_source"
        },
        {
            "base_url": base_url,
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
        },
        {
        'base_url':base_url,
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "fuel_type_data_source"
    }
       
    ]

    # Process API calls and store in MySQL
    f.process_api_calls(api_calls, mysql_credentials, api_key)

    # Execute stored procedure
    f.call_stored_procedure('calculate_co2_reduction', mysql_credentials)
