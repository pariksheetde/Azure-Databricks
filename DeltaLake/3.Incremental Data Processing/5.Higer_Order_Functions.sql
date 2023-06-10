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

CREATE OR REPLACE FUNCTION get_url (email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type (email STRING)
RETURNS STRING

RETURN CASE 
      WHEN email like "%.com" THEN "Commercial Business"
      WHEN email like "%.org" THEN "Non-Proft Business"
      WHEN email like "%.edu" THEN "Educational Institution"
      ELSE concat("Unknown extension for domain: ", split(email, "@")[1])
    END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_domain (email STRING)
RETURNS STRING

RETURN (split(email, "@")[1])

-- COMMAND ----------

SELECT 
email
,get_url(email) as url
,get_domain(email) as domain
,site_type(email) as category
FROM
dw_analytics.customers;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type (email STRING)
RETURNS STRING

RETURN CASE 
      WHEN email like "%.com" THEN "Commercial Business"
      WHEN email like "%.org" THEN "Non-Proft Business"
      WHEN email like "%.edu" THEN "Educational Institution"
      ELSE concat("Unknown extension for domain", split(email, "@")[1])
    END;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
