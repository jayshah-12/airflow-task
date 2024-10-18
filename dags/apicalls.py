

api_calls = [
    {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "daily_electricity_source"
    },
    {
           
            "url": "co2-emissions/co2-emissions-aggregates/data/",
            "params": {
                "frequency": "annual",
                "data[0]": "value"
            },
            "columns": ['period', 'fuel-name', 'state-name', 'value', 'value-units'],
            "table_name": "co2_emission_source"
        },
        
]
