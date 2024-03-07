-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####VALIDATE BRONZE TABLE FROM KAFKA DB

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.bronze;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.bronze WHERE topic = 'orders';

-- COMMAND ----------

SELECT * FROM kafka.bronze WHERE topic = 'customers';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### VALIDATE SILVER / TRANSFORMED DATA FROM KAFKA DB

-- COMMAND ----------

SELECT * FROM kafka.orders_silver;

-- COMMAND ----------

SELECT * FROM kafka.bronze WHERE topic = 'books';

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM kafka.bronze WHERE topic = 'books';

-- COMMAND ----------

SELECT * FROM kafka.books_silver ORDER BY end_date DESC, book_id ASC;

-- COMMAND ----------

SELECT * FROM kafka.customers_silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

DROP DATABASE IF EXISTS kafka CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS kafka;

-- COMMAND ----------

DROP TABLE IF EXISTS kafka.books_silver;
