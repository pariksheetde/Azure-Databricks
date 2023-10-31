-- Databricks notebook source
-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM streaming.books
-- MAGIC ORDER BY book_id DESC;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC  SELECT
-- MAGIC COUNT(*) as cnt,
-- MAGIC SUM(price) as total_cost,
-- MAGIC COUNT(category) as cnt_genre,
-- MAGIC category
-- MAGIC FROM streaming.books
-- MAGIC GROUP BY category;
