# Databricks notebook source
# MAGIC %md
# MAGIC # User-defined Functions (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a UDF?
# MAGIC A user defined function (UDF) is a function provided by the user of a program or environment, in a context where the usual assumption is that functions are built into the program or environment. In relational database management systems, a user-defined function provides a mechanism for extending the functionality of the database server by adding a function, that can be evaluated in standard query language (usually SQL) statements.
# MAGIC 
# MAGIC You use UDFs in PySpark by creating a function in a Python syntax and wrap it with PySpark SQL udf() or register it as udf and use it on DataFrame and SQL respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Types of UDFs:
# MAGIC ### Pandas UDF:
# MAGIC Pandas UDFs are better used for smaller data sets because they only utilize one node for processing.
# MAGIC 
# MAGIC ### PySpark UDF:
# MAGIC PySpark UDFs are better for larger data sets that benefit from using multiple nodes for processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pandas UDF
# MAGIC 
# MAGIC More efficient than pyspark udf.
# MAGIC 
# MAGIC Does not need to be converted to pandas dataframe to use
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Below is a chart from [databricks blog](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html) that compares functions being made using pyspark udf vs pandas udf
# MAGIC 
# MAGIC ![Comparison chart](https://databricks.com/wp-content/uploads/2017/10/image1-4.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## When shouldn't you use a UDF?
# MAGIC While PySpark UDF could be considered the most useful feature of Spark SQL & DataFrame that is used to extend the PySpark build in capabilities, UDF’s are the most expensive operations hence use them only you have no choice and when essential.

# COMMAND ----------

from pyspark.sql.functions import col

targets = spark.sql("SELECT * FROM monthly_all.targets")

targets.printSchema()

# COMMAND ----------

# DBTITLE 0,Pandas UDF Example:
# MAGIC %md
# MAGIC ## Converting a Pythin function into a PySpark UDF

# COMMAND ----------

from pyspark.sql.functions import udf
import pyspark.sql.functions as F

F.udf

df1 = targets


def convertCase(string):
  return string.lower()

# Converting the convertCase() function to a UDF
convertUDF = udf(lambda x: convertCase(x),'string')

# Using select column.
df1.select(col("ticker"), convertUDF(col("ticker")).alias("ticker_lower")).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC # Pyspark UDF (using annotation)

# COMMAND ----------

df = targets
df = df.select(col("ticker"), col("quarter_num"))

@udf('integer')
def plus_one(v):
      return v + 1
df = df.withColumn('int_quarter_num', plus_one(df.quarter_num))

@udf('double')
def plus_one(v):
      return v + 1
df = df.withColumn('double_quarter_num', plus_one(df.quarter_num))

@udf('double')
def plus_one(v):
      return v + 1.0 # must return the corresponding type
df = df.withColumn('double2_quarter_num', plus_one(df.quarter_num))

@udf('string')
def plus_one(v):
      return v + 1
df = df.withColumn('string_quarter_num', plus_one(df.quarter_num))

@udf('float')
def plus_one(v):
      return v + 1
df = df.withColumn('float_quarter_num', plus_one(df.quarter_num))

# @udf('float')
def plus_one(v):
      return v + 1.0 # must return the corresponding type
df = df.withColumn('float2_quarter_num', plus_one(df.quarter_num))

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType

df = targets

@pandas_udf('integer')
def plus_one(v):
      return v + 1
df = df.withColumn('int_quarter_num', plus_one(df.quarter_num))

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark UDF using sql

# COMMAND ----------

def plus_one(v):
  return v+1

# register UDF into spark as a function

spark.udf.register("increment",plus_one)
# spark.sql("SELECT ")
df = spark.sql("SELECT ticker,quarter_end,quarter_num,increment(quarter_num) AS quarter_num_inc FROM monthly_all.targets")

df.createOrReplaceTempView("df_with_inc")

df = spark.sql("SELECT *,increment(quarter_num_inc) as quarter_num_inc_1 FROM df_with_inc")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Another example with UDF
# MAGIC 
# MAGIC Using `explain` we can see that there are more jobs that need to be done using UDF vs no UDF

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.sql("SELECT ticker,quarter_num,quarter_end FROM monthly_all.targets")

# COMMAND ----------

example = df.withColumn("quarter_offset",F.split("quarter_end","-").getItem(1) - 3 * F.col("quarter_num")).filter(F.col("quarter_offset") != 0)
display(example)
example.explain('formatted')

# COMMAND ----------

@F.udf("integer")
def match_quarters(quarter_num,quarter_end):
  year,month,day = str(quarter_end).split("-")
  return min(int(month) - 3 * int(quarter_num), int(month)-3*int(quarter_num) - 12,key=abs)
example = df.withColumn("quarter_offset",match_quarters(F.col("quarter_num"),F.col("quarter_end"))).filter(F.col("quarter_offset") != 0)
display(example)
example.explain('formatted')

# COMMAND ----------



# COMMAND ----------

months = spark.sql('SELECT * FROM monthly_all.monthly_patterns')
def uppercase(s):
  return s.upper()
upperUDF = F.udf(uppercase,'string')
display(months.withColumn('location_name',upperUDF(months.location_name)))
display(months)

# COMMAND ----------

import pandas as pd
from scipy import stats

months = spark.sql('SELECT * FROM monthly_all.monthly_patterns')

@pandas_udf('double')
def cdf(v):
    return pd.Series(stats.norm.cdf(v))


display(months.withColumn('cumulative_probability', cdf(months.median_dwell)))
