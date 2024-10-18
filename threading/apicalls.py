# apicalls.py

api_calls = [
    {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "new_data2"
    },
]
