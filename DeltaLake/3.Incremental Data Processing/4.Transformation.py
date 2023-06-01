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

# MAGIC %md
# MAGIC #### QUERY DW_ANALYTICS.ORDERS TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.orders WHERE order_id = '000000000003559';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.orders WHERE order_id = '000000000004243' LIMIT 10 ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC order_id
# MAGIC ,customer_id
# MAGIC ,explode(books) as books 
# MAGIC FROM 
# MAGIC dw_analytics.orders 
# MAGIC WHERE order_id = '000000000004243' 
# MAGIC LIMIT 10;

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
# MAGIC     FROM dw_analytics.orders
# MAGIC   ) 
# MAGIC temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC customer_id,
# MAGIC collect_set(order_id) AS order_id,
# MAGIC collect_set(books.book_id) AS book_id
# MAGIC FROM dw_analytics.orders
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC o.customer_id,
# MAGIC collect_set(o.books.book_id) AS Before_Flatten,
# MAGIC array_distinct(flatten(collect_set(o.books.book_id))) AS After_Flatten
# MAGIC FROM
# MAGIC dw_analytics.orders o
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC (
# MAGIC SELECT *, explode(books) AS book FROM delta.orders) o
# MAGIC JOIN delta.books_staging b
# MAGIC ON o.book.book_id = b.book_id;
