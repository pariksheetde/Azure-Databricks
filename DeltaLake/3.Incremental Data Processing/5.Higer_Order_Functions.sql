-- Databricks notebook source
-- MAGIC %run ../0.Includes/1.Copy-Datasets

-- COMMAND ----------

SELECT * FROM delta.orders;

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
      FROM delta.orders o)
WHERE size(copies) > 0;

-- COMMAND ----------

SELECT
o.order_id,
o.books,
TRANSFORM(
  books, b -> CAST(b.subtotal * .80 AS INT)
) AS total_after_discount
FROM delta.orders o;
