api_calls = [
    {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "new_data3",
        "dtype": {
            'period': 'DATE',  
            'respondent-name': 'VARCHAR(255)',
            'type-name': 'VARCHAR(255)',
            'value': 'FLOAT',
            'value-units': 'VARCHAR(50)'
        }
    },
    {
        "url": "co2-emissions/co2-emissions-aggregates/data/",
        "params": {
            "frequency": "annual",
            "data[0]": "value"
        },
        "columns": ['period', 'fuel-name', 'state-name', 'value', 'value-units'],
        "table_name": "emission_co2_source",
        "dtype": {
            'period': 'DATE', 
            'fuel-name': 'VARCHAR(255)',
            'state-name': 'VARCHAR(255)',
            'value': 'FLOAT',
            'value-units': 'VARCHAR(50)'
        }
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
        "filter": lambda df: df[
            ((df['activityName'] == 'Generation') & (df['unitName'] == 'billion kilowatthours')) | 
            (df['activityName'] == 'Capacity')
        ],
        "dtype": {
            'period': 'DATE', 
            'productName': 'VARCHAR(255)',
            'activityName': 'VARCHAR(255)',
            'unitName': 'VARCHAR(50)',
            'value': 'FLOAT'
        }
    }
]
