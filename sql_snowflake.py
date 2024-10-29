import mysql.connector
import snowflake.connector
import pandas as pd
 
# Step 1: Connect to MySQL``
mysql_config = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'eia'
}

mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("SELECT * FROM temp_table")
data = mysql_cursor.fetchall()


columns = [i[0] for i in mysql_cursor.description]
print(columns)
snowflake_config = {
    'user': 'JAYSHAH123',
    'password': 'Jayshah12',
    'account': 'jc34516.ap-southeast-1',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_WAREHOUSE',
    'schema': 'MYSCHEMA'
}

snowflake_conn = snowflake.connector.connect(**snowflake_config)
snowflake_cursor = snowflake_conn.cursor()


df = pd.DataFrame(data, columns=columns)

for index, row in df.iterrows():
    insert_query = f"INSERT INTO test ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(row))})"
    snowflake_cursor.execute(insert_query, tuple(row))


snowflake_conn.commit()

# Close the connections
mysql_cursor.close()
mysql_conn.close()
snowflake_cursor.close()
snowflake_conn.close()
