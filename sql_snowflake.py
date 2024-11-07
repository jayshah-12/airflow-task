import pandas as pd
from sqlalchemy import create_engine


mysql_user = 'root'
mysql_password = 'root'
mysql_host = 'localhost'
mysql_database = 'eia'


snowflake_user = 'JAYSHAH123'
snowflake_password = 'Jayshah12'
snowflake_account = 'jc34516.ap-southeast-1'
snowflake_database = 'MY_WAREHOUSE'
snowflake_schema = 'MYSCHEMA'





tables_to_transfer = ['daily_generation_source']  


mysql_engine = create_engine(f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}')

snowflake_engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}')
batch_size=10000

for table in tables_to_transfer:
    with mysql_engine.connect() as mysql_connection:
        query = f'SELECT * FROM {table}' 
        df = pd.read_sql(query, mysql_connection)

    for start in range(0, df.shape[0], batch_size):
        end = min(start + batch_size, df.shape[0])
        chunk = df.iloc[start:end]
        
        with snowflake_engine.connect() as snowflake_connection:
            chunk.to_sql(table, con=snowflake_connection, if_exists='append', index=False)

    print(f"Data for table '{table}' transferred from MySQL to Snowflake successfully!")

print("All tables transferred successfully!")