from sqlalchemy.types import Integer, String, Float, DateTime  # Make sure to import DateTime
import functions as f

if __name__ == "__main__":
    # Configuration
    base_url = "https://api.eia.gov/v2/"
    api_key = 'ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae'

    # MySQL credentials
    mysql_credentials = {
        "username": 'root',
        "password": 'root',
        "host": '192.168.3.112',
        "database": 'eia'
    }

    # API calls configuration
    api_calls = [
         {
            'base_url': base_url,
            "url": "electricity/rto/daily-fuel-type-data/data/",
            "params": {
                "frequency": "daily",
                "data[0]": "value"
            },
            "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
            "data_types": {
                'period': DateTime(),  # Use DateTime for datetime
                'respondent-name': String(50),  # Corrected key name
                'type-name': String(50),
                'value': Float(),
                'value-units': String(100)
            },
            "table_name": "y"
        },
        {
            "base_url": base_url,
            "url": "co2-emissions/co2-emissions-aggregates/data/",
            "params": {
                "frequency": "annual",
                "data[0]": "value"
            },
            "columns": ['period', 'fuel-name', 'state-name', 'value', 'value-units'],
            "table_name": "abcd",
            "data_types": {
                'period': Integer(),  # Use DateTime for datetime
                'fuel-name': String(50),
                'state-name': String(50),
                'value': Float(),
                'value-units': String(100)
            }
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
            "data_types": {
                'period': Integer(),  # Use DateTime for datetime
                'productName': String(50),
                'activityName': String(50),
                'unitName': String(100),
                'value': Float()
            },
            "table_name": "x",
            "filter": lambda df: df[((df['activityName'] == 'Generation') & (df['unitName'] == 'billion kilowatthours')) | (df['activityName'] == 'Capacity')]
        }
       
    ]

    # Process API calls and store in MySQL
    f.process_api_calls(api_calls, mysql_credentials, api_key)

    # Execute stored procedure
    f.call_stored_procedure('calculate_co2_reduction', mysql_credentials)
