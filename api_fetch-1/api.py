
import functions
import param
# import temp
from sqlalchemy import create_engine, text

mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
engine = create_engine(mysql_connection_string)




# data_api1 = functions.fetch_data(param.api1_url, param.api1_params,no_of_records=2000)
# df1 = data_api1[temp.dict['api_1']['columns']]
# print(df1)
# functions.mysql_connect(df1,"emission_co2_source")

# print(temp.dict['api_1']['columns']

data_api2 = functions.fetch_data(param.api2_url, param.api2_params)  
# df2=data_api2[['period','respondent-name','type-name','value','value-units']]
# functions.mysql_connect(df2,"daily_electricity_source")



# data_api3 = functions.fetch_data(param.api3_url,param.api3_params)
# df3 = data_api3[['period','productName','activityName','unitName','value']]
# df3 = df3[((df3['activityName'] == 'Generation') & (df3['unitName'] == 'billion kilowatthours')) | (df3['activityName'] == 'Capacity')]
# functions.mysql_connect(df3,"renewable_generation_source")


# functions.call_stored_procedure()


# # make a generic function
# # import function as module
# print(temp.dict['api_1'])