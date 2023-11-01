# Databricks notebook source
# MAGIC %md
# MAGIC #### QUERY DW_ANALYTICS.CUSTOMERS TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.customers ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED dw_analytics.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC #### QUERY DW_ANALYTICS.CUSTOMERS TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC c.customer_id,
# MAGIC c.email,
# MAGIC c.profile:first_name,
# MAGIC c.profile:last_name,
# MAGIC c.profile:gender,
# MAGIC c.profile:address,
# MAGIC c.profile:address:street,
# MAGIC c.profile:address:city,
# MAGIC c.profile:address:country
# MAGIC FROM dw_analytics.customers c
# MAGIC ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC c.customer_id,
# MAGIC c.profile:address,
# MAGIC c.profile:address:street,
# MAGIC c.profile:address:city,
# MAGIC c.profile:address:country
# MAGIC FROM dw_analytics.customers c
# MAGIC ORDER BY 1 ASC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
