# Databricks notebook source
dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("destfolder","netflix_directors")

# COMMAND ----------

var_src_folder=dbutils.widgets.get("sourcefolder")
var_dest_folder=dbutils.widgets.get("destfolder")

# COMMAND ----------

print(var_dest_folder)

# COMMAND ----------

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(f"abfss://bronze@netprojectstorageaccount.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("path",f"abfss://silver@netprojectstorageaccount.dfs.core.windows.net/{var_dest_folder}")\
    .save()

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://bronze@netprojectstorageaccount.dfs.core.windows.net/{var_src_folder}"))

# COMMAND ----------

print(df.count())

# COMMAND ----------

try:
    df.write \
      .format("delta") \
      .mode("append") \
      .option("path", f"abfss://silver@netprojectstorageaccount.dfs.core.windows.net/{var_dest_folder}") \
      .save()

    print("SUCCESS")

except Exception as e:
    print(e)

# COMMAND ----------

display(dbutils.fs.ls("abfss://silver@netprojectstorageaccount.dfs.core.windows.net/"))

# COMMAND ----------

display(dbutils.fs.ls(
    "abfss://silver@netprojectstorageaccount.dfs.core.windows.net/netflix_directors"
))