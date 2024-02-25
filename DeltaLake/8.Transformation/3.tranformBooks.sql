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

SELECT 
order_id
,customer_id
,explode(books) as books 
FROM 
dw_analytics.orders 
WHERE order_id = '000000000004243' 
LIMIT 10;

-- COMMAND ----------

SELECT
order_id,
order_timestamp,
customer_id,
quantity,
explode(books) as books
FROM 
  (
    SELECT
    order_id,
    order_timestamp,
    customer_id,
    quantity,
    books
    FROM dw_analytics.orders
  ) 
temp;

-- COMMAND ----------

SELECT
customer_id,
collect_set(order_id) AS order_id,
collect_set(books.book_id) AS book_id
FROM dw_analytics.orders
GROUP BY customer_id;

-- COMMAND ----------

SELECT
o.customer_id,
collect_set(o.books.book_id) AS Before_Flatten,
array_distinct(flatten(collect_set(o.books.book_id))) AS After_Flatten
FROM
dw_analytics.orders o
GROUP BY customer_id;

-- COMMAND ----------

SELECT * FROM dw_analytics.orders LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE VIEW dw_analytics.books_ordered
AS
SELECT * FROM
(
SELECT *, explode(books) AS book FROM dw_analytics.orders) o
JOIN dw_analytics.books b
ON o.book.book_id = b.book_id;

SELECT 
order_id
,book_id
,title
,author
,category
,order_timestamp
,customer_id
,quantity
,price
,total
FROM dw_analytics.books_ordered
WHERE order_id = '000000000004243';

-- COMMAND ----------

SELECT * FROM
(
  SELECT
    customer_id
    ,book.book_id as book_id
    ,book.quantity as quantity
    FROM dw_analytics.books_ordered
) PIVOT(
  sum(quantity) FOR book_id IN ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'B12')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
