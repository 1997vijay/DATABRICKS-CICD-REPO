# Databricks notebook source
# Gather relevant keys from our secret scope

#secrets name from azure key vault
scope_name="dbx-key"
client_id="Client-ID"
directory_id="Tenet-ID"
app_reg_id="DBX-APP-KEY"

#Storage and container name
storage_name='bigdatastoragedev' # your storage name
container_name='alchemy' # your container name

#mount path in the databricks
mount_path=f"/mnt/{container_name}/"

#ClientID from key vault
ClientID = dbutils.secrets.get(scope =scope_name, key = client_id)

#DirectoryID from key vault
DirectoryID = dbutils.secrets.get(scope =scope_name, key =directory_id)

#App Registration from key vault
APP_KEY=dbutils.secrets.get(scope =scope_name, key =app_reg_id)

# # Combine DirectoryID into full string
Directory = f"https://login.microsoftonline.com/{DirectoryID}/oauth2/token"

# Create configurations for our connection
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": ClientID,
"fs.azure.account.oauth2.client.secret": APP_KEY,
"fs.azure.account.oauth2.client.endpoint": Directory}

dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net",
mount_point = mount_path,
extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt'))

# COMMAND ----------

# reusable function for mounting the path

def MoutnAzureDataLake(scope_name,client_id,directory_id,app_reg_id,storage_name,container_name):
    # Gather relevant keys from our secret scope
    #ClientID from key vault
    ClientID = dbutils.secrets.get(scope =scope_name, key = client_id)
    
    #DirectoryID from key vault
    DirectoryID = dbutils.secrets.get(scope =scope_name, key =directory_id)
    
    #App Registration from key vault
    APP_KEY=dbutils.secrets.get(scope =scope_name, key =app_reg_id)
    
    # # Combine DirectoryID into full string
    Directory = f"https://login.microsoftonline.com/{DirectoryID}/oauth2/token"
    
    # defining the mount path
    mount_path=f"/mnt/{container_name}/"
    
    # Create configurations for our connection
    configs = {"fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": ClientID,
    "fs.azure.account.oauth2.client.secret": APP_KEY,
    "fs.azure.account.oauth2.client.endpoint": Directory}
    
    try:
        Mount=dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net",
        mount_point = mount_path,
        extra_configs = configs)
        print(f'{container_name} mounted successfully at {mount_path}')
    except Exception as e:
        print(f'Error-{e}')
        raise

# COMMAND ----------


