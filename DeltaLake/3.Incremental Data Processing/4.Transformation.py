# Databricks notebook source
# MAGIC %run ../0.Includes/1.Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.customers ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED delta.customers; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC c.customer_id,
# MAGIC c.email,
# MAGIC c.profile:first_name,
# MAGIC c.profile:last_name,
# MAGIC c.profile:gender,
# MAGIC c.profile:address:street,
# MAGIC c.profile:address:city,
# MAGIC c.profile:address:country
# MAGIC FROM delta.customers c
# MAGIC ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.orders WHERE customer_id = 'C00001';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC order_id,
# MAGIC order_timestamp,
# MAGIC customer_id,
# MAGIC quantity,
# MAGIC explode(books) as books
# MAGIC FROM 
# MAGIC   (
# MAGIC     SELECT
# MAGIC     order_id,
# MAGIC     order_timestamp,
# MAGIC     customer_id,
# MAGIC     quantity,
# MAGIC     books
# MAGIC     FROM delta.orders
# MAGIC   ) 
# MAGIC temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC customer_id,
# MAGIC collect_set(order_id) AS order_id,
# MAGIC collect_set(books.book_id) AS book_id
# MAGIC FROM delta.orders
# MAGIC GROUP BY customer_id;

# COMMAND ----------


