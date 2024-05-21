# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

dl_races_df = spark.read \
.parquet(f"{processed_path}/races")

display(dl_races_df)
dl_races_df.printSchema()
print(f"Number of Records Read {dl_races_df.count()}")
print(processed_path)

# COMMAND ----------

from pyspark.sql.functions import col

filtered_races_df = dl_races_df.filter("race_year = 2019 and name like 'Australian%'")
display(filtered_races_df)

# COMMAND ----------

from pyspark.sql.functions import col

filtered_races_df = dl_races_df.filter((dl_races_df["race_year"] == 2019) & (dl_races_df["name"] != "Australian Grand Prix"))
display(filtered_races_df)

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
