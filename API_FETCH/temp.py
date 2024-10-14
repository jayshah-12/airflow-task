dict = {
    'api_1' : {
        "url": "co2-emissions/co2-emissions-aggregates/data/",
        "params": {
            "frequency": "annual",
            "data[0]": "value"
        },
        "columns": "['period', 'fuel-name', 'state-name', 'value', 'value-units']",
        "table_name": "emission_co2_source"
        },
    'api_2': {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": "['period', 'respondent-name', 'type-name', 'value', 'value-units']",
        "table_name": "daily_electricity_source",
        "no_of_records": 200000
    },
    'api_3': {
        "url": "international/data/",
        "params": {
            "frequency": "annual",
            "data[0]": "value",
            "facets[productId][]": [116, 33, 37],
            "facets[countryRegionId][]": "USA"
        },
        "columns":"['period', 'productName', 'activityName', 'unitName', 'value']",
        "table_name": "renewable_generation_source",
        "filter": "lambda df: df[((df['activityName'] == 'Generation') & (df['unitName'] == 'billion kilowatthours')) | (df['activityName'] == 'Capacity')]"
    }

}