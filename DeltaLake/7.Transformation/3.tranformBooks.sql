-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY DW_ANALYTICS.ORDERS TABLE

-- COMMAND ----------

SELECT * FROM dw_analytics.orders WHERE order_id = '000000000003559';

-- COMMAND ----------

SELECT * FROM dw_analytics.orders WHERE order_id = '000000000004243' LIMIT 10 ;

-- COMMAND ----------

DESC EXTENDED dw_analytics.orders;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC order_id
-- MAGIC ,customer_id
-- MAGIC ,explode(books) as books 
-- MAGIC FROM 
-- MAGIC dw_analytics.orders 
-- MAGIC WHERE order_id = '000000000004243' 
-- MAGIC LIMIT 10;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC order_id,
-- MAGIC order_timestamp,
-- MAGIC customer_id,
-- MAGIC quantity,
-- MAGIC explode(books) as books
-- MAGIC FROM 
-- MAGIC   (
-- MAGIC     SELECT
-- MAGIC     order_id,
-- MAGIC     order_timestamp,
-- MAGIC     customer_id,
-- MAGIC     quantity,
-- MAGIC     books
-- MAGIC     FROM dw_analytics.orders
-- MAGIC   ) 
-- MAGIC temp;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC customer_id,
-- MAGIC collect_set(order_id) AS order_id,
-- MAGIC collect_set(books.book_id) AS book_id
-- MAGIC FROM dw_analytics.orders
-- MAGIC GROUP BY customer_id;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC o.customer_id,
-- MAGIC collect_set(o.books.book_id) AS Before_Flatten,
-- MAGIC array_distinct(flatten(collect_set(o.books.book_id))) AS After_Flatten
-- MAGIC FROM
-- MAGIC dw_analytics.orders o
-- MAGIC GROUP BY customer_id;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM dw_analytics.orders LIMIT 10;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW dw_analytics.books_ordered
-- MAGIC AS
-- MAGIC SELECT * FROM
-- MAGIC (
-- MAGIC SELECT *, explode(books) AS book FROM dw_analytics.orders) o
-- MAGIC JOIN dw_analytics.books b
-- MAGIC ON o.book.book_id = b.book_id;
-- MAGIC
-- MAGIC SELECT 
-- MAGIC order_id
-- MAGIC ,book_id
-- MAGIC ,title
-- MAGIC ,author
-- MAGIC ,category
-- MAGIC ,order_timestamp
-- MAGIC ,customer_id
-- MAGIC ,quantity
-- MAGIC ,price
-- MAGIC ,total
-- MAGIC FROM dw_analytics.books_ordered
-- MAGIC WHERE order_id = '000000000004243';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM
-- MAGIC (
-- MAGIC   SELECT
-- MAGIC     customer_id
-- MAGIC     ,book.book_id as book_id
-- MAGIC     ,book.quantity as quantity
-- MAGIC     FROM dw_analytics.books_ordered
-- MAGIC ) PIVOT(
-- MAGIC   sum(quantity) FOR book_id IN ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'B12')
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")