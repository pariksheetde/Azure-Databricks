# Databricks notebook source
# MAGIC %md
# MAGIC #### ENTER THE BELOW DETAILS
# MAGIC ## client_id
# MAGIC ##### secret_id
# MAGIC ##### tenant_id

# COMMAND ----------

storage_account_name = "adobeadls"
client_id = "fd5f716f-1161-44f8-a45d-738c8ae7a59a"
client_secret = "O-18Q~C~zL-~cs3_QP3gEfSLfTAOLJM~Z2i3obDP"
tenant_id = "9cd5292d-d337-4834-b68a-15f1ebfcf00c"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### create a UDF to mount the container in adls

# COMMAND ----------

def mount_adls(container_name):
  storage_name = "adobeadls"
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### mount raw container

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### mount processed container

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### mount etl container

# COMMAND ----------

mount_adls("etl")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### mount presentation container

# COMMAND ----------

mount_adls("incremental")

# COMMAND ----------

mount_adls("deltalake")

# COMMAND ----------

dbutils.fs.ls("/mnt/adobeadls/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/raw")
dbutils.fs.unmount("/mnt/adobeadls/processed")
dbutils.fs.unmount("/mnt/adobeadls/presentation")
dbutils.fs.unmount("/mnt/adobeadls/incremental")

# COMMAND ----------

dbutils.fs.ls("/mnt/adobeadls/presentation")

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/presentation")
