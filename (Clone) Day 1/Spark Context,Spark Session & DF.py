# Databricks notebook source
# MAGIC %md
# MAGIC Demo on Spark Context 

# COMMAND ----------

spark

# COMMAND ----------

rdd=sc.parallelize([1,2,3])

# COMMAND ----------

rdd.collect()

# COMMAND ----------

df=spark.createDataFrame([(1,2,3)])

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC We can create DataFrame by using 2 ways
# MAGIC - 1. if you have a data
# MAGIC - 2. if you have a file(csv, json, parquet,etc)

# COMMAND ----------

users_data=[(1,'a',30),(2,'b',29)]

# COMMAND ----------

df=spark.createDataFrame(data=users_data)

# COMMAND ----------

df.show()

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema
# MAGIC 1. why we need schema
# MAGIC   - no Headers
# MAGIC   - correct order
# MAGIC
# MAGIC 2. Which type of schema
# MAGIC   - Str
# MAGIC   - list
# MAGIC   - pyspark
# MAGIC

# COMMAND ----------

schema_str="id int, name string, age int"

# COMMAND ----------

df=spark.createDataFrame(data=users_data, schema=schema_str )

# COMMAND ----------

df=spark.createDataFrame(users_data, schema_str )

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC #### DBFS: 
# MAGIC An Abstraction of your object storage

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Utilities

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/michelinraw/

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/michelinraw/emp.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/michelinraw/emp.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/michelinraw/emp.csv")

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------


