from sqlalchemy.types import Date, Float, String, Integer

api_calls = [
    {
        "url": "electricity/rto/daily-fuel-type-data/data/",
        "params": {
            "frequency": "daily",
            "data[0]": "value"
        },
        "columns": ['period', 'respondent-name', 'type-name', 'value', 'value-units'],
        "table_name": "a",
        'dtype':{
        'period': Date(),
        'respondent-name': String(255),
        'type-name': String(255),
        'value': Float(),
        'value-units': String(50)
}
    },
    {
        "url": "co2-emissions/co2-emissions-aggregates/data/",
        "params": {
            "frequency": "annual",
            "data[0]": "value"
        },
        "columns": ['period', 'fuel-name', 'state-name', 'value', 'value-units'],
        "table_name": "b",
       'dtype':{
        'period': Integer(),
        'fuel-name': String(255),
        'state-name': String(255),
        'value': Float(),
        'value-units': String(255)
}
    },
#     {
#         "url": "international/data/",
#         "params": {
#             "frequency": "annual",
#             "data[0]": "value",
#             "facets[productId][]": [116, 33, 37],
#             "facets[countryRegionId][]": "USA"
#         },
#         "columns": ['period', 'productName', 'activityName', 'unitName', 'value'],
#         "table_name": "c",
#         'dtype':{
#         'period': Integer(),
#         'productName': String(255),
#         'activityName': String(255),
#         'unitName': String(255),
#         'value': Float(10)
# }
#     }
]
