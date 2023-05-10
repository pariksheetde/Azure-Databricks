-- Databricks notebook source
-- MAGIC %run ../0.Includes/1.Copy-Datasets

-- COMMAND ----------

SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.orders;
CREATE TABLE IF NOT EXISTS dw_analytics.orders
AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

SELECT * FROM dw_analytics.orders;

-- COMMAND ----------

SELECT count(*) AS cnt FROM dw_analytics.orders;

-- COMMAND ----------

INSERT OVERWRITE dw_analytics.orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

SELECT count(*) AS cnt FROM dw_analytics.orders;

-- COMMAND ----------

DESC HISTORY dw_analytics.orders;

-- COMMAND ----------

SELECT count(*) AS cnt FROM dw_analytics.orders;

-- COMMAND ----------

SELECT
order_id
,order_timestamp
,books
,explode(books)
,
FROM
dw_analytics.orders
ORDER BY 1 ASC;

-- COMMAND ----------

SELECT
customer_id
,collect_set(order_id) as order_id
,collect_set(books.book_id) as book_set
FROM
dw_analytics.orders
GROUP BY customer_id
ORDER BY 1 ASC;

-- COMMAND ----------

SELECT
customer_id
,collect_set(order_id) as order_id
,collect_set(books.book_id) as book_set_before_flatten
,array_distinct(flatten(collect_set(books.book_id))) as book_set_after_flatten
FROM
dw_analytics.orders
GROUP BY customer_id
ORDER BY 1 ASC;

-- COMMAND ----------

SELECT
*
FROM (
  SELECT *, explode(books) as book
  FROM dw_analytics.orders) o JOIN dw_analytics.books b
  ON o.book.book_id = b.book_id;

-- COMMAND ----------

SELECT *, explode(books) as book
  FROM dw_analytics.orders;

-- COMMAND ----------

DESC EXTENDED dw_analytics.orders;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
