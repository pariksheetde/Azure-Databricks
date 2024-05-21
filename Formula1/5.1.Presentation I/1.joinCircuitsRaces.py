# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ circuits.parquet FROM DATALAKE (PROCESSED DATA)

# COMMAND ----------

dl_circuits_df = spark.read \
.parquet(f"{processed_path}/circuits") \
.withColumnRenamed("name", "circuit_name")

display(dl_circuits_df)
print(f"Number of Records Read {dl_circuits_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ races.parquet FROM DATALAKE (PROCESSED DATA)

# COMMAND ----------

dl_races_df = spark.read \
.parquet(f"{processed_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

display(dl_races_df)
print(f"Number of Records Read {dl_races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER JOIN

# COMMAND ----------

race_circuit_df = dl_circuits_df.join(dl_races_df, dl_circuits_df.circuit_id == dl_races_df.circuit_id, "inner") \
.select("circuit_name", "location", "country", "race_name" ,"round")

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SEMI JOIN

# COMMAND ----------

race_circuit_df = dl_circuits_df.join(dl_races_df, dl_circuits_df.circuit_id == dl_races_df.circuit_id, "semi") \
.select("circuit_name", "circuit_ref", "country", "location")

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOIN

# COMMAND ----------

race_circuit_df = dl_circuits_df.join(dl_races_df, dl_circuits_df.circuit_id == dl_races_df.circuit_id, "anti") \
.select("circuit_name", "circuit_ref", "country", "location")

display(race_circuit_df)

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
