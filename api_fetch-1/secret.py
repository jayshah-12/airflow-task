import hvac


VAULT_ADDR = 'http://localhost:8200'
VAULT_TOKEN = 'root' 

client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)

secret_path = 'myapp/creds'  

secret = client.secrets.kv.v2.read_secret_version(path=secret_path, raise_on_deleted_version=True)

# print(secret['metadata']['created_time'])
api_key = secret['data']['data']['api_key']
# sql_pass = secret['data']['data']['sql_pass']
# sql_user = secret['data']['data']['sql_user']
print(api_key)

# print(sql_pass, sql_user)


