# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as F

df = spark.sql("SELECT location_name,related_same_day_brand FROM monthly_all.monthly_patterns")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample solution

# COMMAND ----------

df = spark.sql("SELECT location_name,related_same_day_brand FROM monthly_all.monthly_patterns")

@udf('string')
def getTopDailyVisits(visits):
  top = ["", int()]
  if visits is None:
    return None
  for key, value in visits.items():
    if value > top[1]:
      top[1] = value
      top[0] = f"{key}:"
    if top[1] <= 0:
      return None
  else:
    return " ".join(str(x) for x in top)
  
display(df.withColumn('maxLocation',getTopDailyVisits(df.related_same_day_brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Class Problem
# MAGIC 
# MAGIC Using `df` from below, use a UDF to find the day (based on index) that has the max value within visits_by_day

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as F

df = spark.sql("SELECT location_name,visits_by_day FROM monthly_all.monthly_patterns")
display(df)

# COMMAND ----------

# MAGIC %md
