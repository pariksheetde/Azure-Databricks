-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####VALIDATE BRONZE TABLE FROM KAFKA DB

-- COMMAND ----------

-- DROP DATABASE IF EXISTS kafka CASCADE;
-- CREATE DATABASE IF NOT EXISTS kafka;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.bronze;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.orders_silver;

-- COMMAND ----------

SELECT * FROM kafka.orders_silver;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.books_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### VALIDATE SILVER / TRANSFORMED DATA FROM KAFKA DB

-- COMMAND ----------

SELECT * FROM kafka.books_silver ORDER BY book_id ASC;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.customers_silver;

-- COMMAND ----------

SELECT * FROM kafka.customers_silver;

-- COMMAND ----------

SELECT * FROM
table_changes("kafka.customers_silver", 1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

SELECT * FROM kafka.books_silver_rowtime;
