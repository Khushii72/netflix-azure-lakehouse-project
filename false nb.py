# Databricks notebook source
# DBTITLE 1,Cell 1
var = dbutils.jobs.taskValues.get(taskKey="weekday_lookup", key="weekoutput", debugValue="")

# COMMAND ----------

print(var)