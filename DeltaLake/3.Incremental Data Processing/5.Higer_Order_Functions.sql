-- Databricks notebook source
SELECT * FROM dw_analytics.orders LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### FILTER lambda function to filter out unwanted records

-- COMMAND ----------

SELECT
  order_id,
  books,
  copies
  FROM
  (
    SELECT
      o.order_id,
      o.books,
      FILTER(books, i -> i.quantity >=2) AS copies
      FROM dw_analytics.orders o)
WHERE size(copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TRANSFORM lambda function to transform records

-- COMMAND ----------

SELECT
o.order_id,
o.books,
TRANSFORM(
  books, b -> CAST(b.subtotal * .80 AS INT)
) AS total_after_discount
FROM dw_analytics.orders o;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
