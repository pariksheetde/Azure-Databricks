-- Databricks notebook source
SELECT *
FROM streaming.books
ORDER BY book_id DESC;

-- COMMAND ----------

 SELECT
COUNT(*) as cnt,
SUM(price) as total_cost,
COUNT(category) as cnt_genre,
category
FROM streaming.books
GROUP BY category;
