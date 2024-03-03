-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### VALIDATE BRONZE TABLE FROM KAFKA DB

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.bronze WHERE topic = 'orders';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### VALIDATE SILVER / TRANSFORMED DATA FROM KAFKA DB

-- COMMAND ----------

SELECT * FROM kafka.orders_silver;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.bronze WHERE topic = 'books';

-- COMMAND ----------

SELECT * FROM kafka.books_silver;
