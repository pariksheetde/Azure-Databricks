# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta.books;
# MAGIC CREATE TABLE delta.books
# MAGIC (book_id INT, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC header = "true",
# MAGIC delimeter = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/mnt/adobeadls/raw/top_directors.csv`

# COMMAND ----------

# from pyspark.sql.types import StringType, StructField, StructType, IntegerType
# schema = StructType(
# [
#     StructField("ID", IntegerType(),True),
#     StructField("Directors", StringType(),True),
#     StructField("Year", IntegerType(),True),
#     StructField("Movies", StringType(),True),
# ]
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta.top_director_csv;
# MAGIC CREATE TABLE delta.top_director_csv
# MAGIC (id INT, Directors STRING, Year INT, Movies STRING)
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC header = "true"
# MAGIC )
# MAGIC LOCATION "/mnt/adobeadls/raw/top_directors.csv";

# COMMAND ----------

# spark.readStream \
# .schema(schema) \
# .option("header", "true") \
# .csv("/mnt/adobeadls/raw/top_directors.csv") \
# .createOrReplaceGlobalTempView("top_director_csv_temp_vw")

# COMMAND ----------



# COMMAND ----------

# %sql
# SELECT * FROM global_temp.top_director_csv_temp_vw;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE delta.top_director_csv
