# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### CONSTRUCTORS'S STANDING

# COMMAND ----------

from pyspark.sql.functions import *

constructors_standing_df = race_results_df.groupBy("race_year", "team") \
.agg(
    sum("points").alias("sum_points"),
    count(when(col("position") == 1, True)).alias("wins")
   )
display(constructors_standing_df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowSpec = Window \
    .partitionBy("race_year") \
    .orderBy(col("sum_points").desc(), col("wins").desc())

constructors_rank_spec_df = constructors_standing_df.select("race_year", "team", "sum_points", "wins") \
.filter("race_year = 2020") \
.withColumn("rank", rank().over(windowSpec))

display(constructors_rank_spec_df)

# COMMAND ----------

constructors_rank_spec_df.write.mode("overwrite").parquet(f"{presentation_path}/constructors_standing")

# COMMAND ----------

print(f"Number of Records Effected: {constructors_rank_spec_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE THE DATA IN THE PRESENTATION DB

# COMMAND ----------

constructors_rank_spec_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructors_standing")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.constructors_standing;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
