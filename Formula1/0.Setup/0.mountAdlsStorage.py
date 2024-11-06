# Databricks notebook source
# MAGIC %md
# MAGIC #### ENTER THE BELOW DETAILS
# MAGIC 1. client_id
# MAGIC 2. secret_id
# MAGIC 3. tenant_id

# COMMAND ----------

# Ensure that the Databricks cluster has the necessary permissions to access the ADLS path
# You can set up the necessary permissions by configuring the Azure Data Lake Storage credentials

# Example: Setting up the credentials using a service principal
spark.conf.set("fs.azure.account.auth.type.adobeadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adobeadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adobeadls.dfs.core.windows.net", "3593ecd6-5801-4ab0-9e2c-6b00d3435c75")
spark.conf.set("fs.azure.account.oauth2.client.secret.adobeadls.dfs.core.windows.net", "W1d8Q~SNsnCtOmmB8seSZR3qrg6wQV1sQ.BSjaUk")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adobeadls.dfs.core.windows.net", "https://login.microsoftonline.com/9cd5292d-d337-4834-b68a-15f1ebfcf00c/oauth2/token")

# COMMAND ----------

storage_account_name = "adobeadls"
client_id = "3593ecd6-5801-4ab0-9e2c-6b00d3435c75"
client_secret = "W1d8Q~SNsnCtOmmB8seSZR3qrg6wQV1sQ.BSjaUk"
tenant_id = "9cd5292d-d337-4834-b68a-15f1ebfcf00c"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a UDF to mount the container in adls

# COMMAND ----------

def mount_adls(container_name):
  storage_name = "adobeadls"
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/raw")
mount_adls("raw")
dbutils.fs.ls("/mnt/adobeadls/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Processed Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/processed")
mount_adls("processed")
dbutils.fs.ls("/mnt/adobeadls/processed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Retail Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/retail")
mount_adls("retail")
dbutils.fs.ls("/mnt/adobeadls/retail")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Presentation Container
# MAGIC

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/presentation")
mount_adls("presentation")
dbutils.fs.ls("/mnt/adobeadls/presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Incremental Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/incremental")
mount_adls("incremental")
dbutils.fs.ls("/mnt/adobeadls/presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Dwanalytics Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/dwanalytics")
mount_adls("dwanalytics")
dbutils.fs.ls("/mnt/adobeadls/dwanalytics")

# COMMAND ----------

dbutils.fs.unmount("/mnt/adobeadls/deltalake")
mount_adls("deltalake")
dbutils.fs.ls("/mnt/adobeadls/deltalake")

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")