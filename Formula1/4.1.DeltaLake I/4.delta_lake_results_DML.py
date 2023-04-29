# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Update Delta Table (f1_delta.results_managed)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_delta.results_managed
# MAGIC SET points = points = 0
# MAGIC WHERE constructorid = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed ORDER BY 1 ASC;

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

results_managed = DeltaTable.forPath(spark, "/mnt/covidanalysisadls/deltalake/results_external")
results_managed.update("constructorid = 1", {"points" : "100"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_delta.results_external WHERE constructorid = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external ORDER BY 1 ASC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
