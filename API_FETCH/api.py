
import functions
import param





data_api1 = functions.fetch_data(param.api1_url, param.api1_params)
df1 = data_api1[['period','fuel-name', 'state-name','value','value-units']]
functions.mysql_connect(df1,"emission_co2_source")



data_api2 = functions.fetch_data(param.api2_url, param.api2_params, no_of_records=200000)  
df2=data_api2[['period','respondent-name','type-name','value','value-units']]
functions.mysql_connect(df2,"daily_electricity_source")



data_api3 = functions.fetch_data(param.api3_url,param.api3_params, no_of_records=10)
df3 = data_api3[['period','productName','activityName','unitName','value']]
df3 = df3[((df3['activityName'] == 'Generation') & (df3['unitName'] == 'billion kilowatthours')) | (df3['activityName'] == 'Capacity')]
functions.mysql_connect(df3,"renewable_generation_source")


functions.call_stored_procedure()
