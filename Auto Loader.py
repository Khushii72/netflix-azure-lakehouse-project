# Databricks notebook source
checkpoint_location = "abfss://silver@netprojectstorageaccount.dfs.core.windows.net/checkpoint1"

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load("abfss://raw@netprojectstorageaccount.dfs.core.windows.net/folder2")

# COMMAND ----------

# DBTITLE 1,Cell 3

display(df, checkpointLocation=checkpoint_location)

# COMMAND ----------

dbutils.fs.ls("abfss://raw@netprojectstorageaccount.dfs.core.windows.net")

# COMMAND ----------

df.writeStream\
  .format("delta")\
  .option("checkpointLocation",checkpoint_location)\
  .trigger(processingTime='10 seconds')\
  .start("abfss://bronze@netprojectstorageaccount.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="10 seconds")
    .start("abfss://bronze@netprojectstorageaccount.dfs.core.windows.net/netflix_titles")
)

query.status

# COMMAND ----------

print(df.isStreaming)

# COMMAND ----------

query.status

# COMMAND ----------

query.exception()

# COMMAND ----------

query.lastProgress

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@netprojectstorageaccount.dfs.core.windows.net/folder1"))

# COMMAND ----------

display(dbutils.fs.ls(checkpoint_location))

# COMMAND ----------

batch_df = (
    spark.read
    .option("header", "true")
    .csv("abfss://raw@netprojectstorageaccount.dfs.core.windows.net/folder1")
)

print(batch_df.count())
batch_df.printSchema()
display(batch_df.limit(5))