# Databricks notebook source
# MAGIC %md 
# MAGIC # Using Auto Loader to simplify ingest as the first step in your ETL
# MAGIC 
# MAGIC ### Overview
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html) is an ingest feature of Databricks that makes it simple to incrementally ingest only new data from Azure Data Lake. In this notebook we will use Auto Loader for a basic ingest use case but there are many features of Auto Loader, like [schema inference and evolution](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html#schema-inference-and-evolution), that make it possible to ingest very complex and dynymically changing data.
# MAGIC 
# MAGIC The following example ingests financial data. Estimated Earnings Per Share (EPS) is financial data from analysts predicting what a company’s quarterly earnings per share will be. The raw data can come from many different sources and from multiple analysts for multiple stocks. In this notebook, the data is simply ingested into the bronze table using Auto Loader.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/jodb/DatabricksAndAzureMapsWorkshop/master/episode4/azureDbIngestEtlArch.png">

# COMMAND ----------

# DBTITLE 1,Set up storage paths
# autoloader table and checkpoint paths
basepath = "dbfs:/mnt/tutorial/incrementalETL/"
bronzeTable = basepath + "bronze/"
bronzeCheckpoint = basepath + "bronze/checkpoint/"
bronzeSchema = basepath + "bronze/schema/"
silverTable = basepath + "silver/"
silverCheckpoint = basepath + "silver/checkpoint/"
landingZoneLocation = basepath + "LandingZone/"

# COMMAND ----------

# DBTITLE 1,Remove data from the base path, comment out if you would like to run this
# dbutils.fs.rm(basepath, recurse = True)

# COMMAND ----------

# DBTITLE 1,Code used to build the 1st Estimated Earnings Per Share data set to import into the table
df = spark.createDataFrame(
    [('3/1/2021','a',1,2.2),\
     ('3/1/2021','a',2,2.0),\
     ('3/1/2021','b',1,1.3),\
     ('3/1/2021','b',2,1.2),\
     ('3/1/2021','c',1,3.5),\
     ('3/1/2021','c',2,2.6)],
    ('date','stock_symbol','analyst','estimated_eps'))
# write one new csv file to the landing zone
df.repartition(1).write.mode("append").option("header","true").csv(landingZoneLocation)

# COMMAND ----------

# MAGIC %md 
# MAGIC In the code below we use [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html) to ingest the data into the bronze table and well as [schemaLocation](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-inference) which allows us to infer the schema instead of having to define it ahead of time.
# MAGIC 
# MAGIC In the writing of the data, we use [trigger.once](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) and [checkpointing](https://docs.databricks.com/spark/latest/structured-streaming/production.html?_ga=2.205141207.63998281.1636395861-1297538452.1628713993#enable-checkpointing) both of which make Incremental ETL easy which you can read more about in [this blog](https://databricks.com/blog/2021/08/30/how-incremental-etl-makes-life-simpler-with-data-lakes.html).

# COMMAND ----------

# DBTITLE 1,Trigger once stream from autoloader to bronze table
# "cloudFiles" indicates the use of Auto Loader
# We are using csv and give a schema location so that the current schema can be saved and infered easily
dfBronze = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", bronzeSchema) \
  .load(landingZoneLocation)

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
# dfBronze.writeStream \
#   .format("delta") \
#   .trigger(once=True) \
#   .option("checkpointLocation", bronzeCheckpoint) \
#   .start(bronzeTable)

# COMMAND ----------

dfBronze = spark.read.format("delta").load(bronzeTable)
display(dfBronze.orderBy("date", "stock_symbol", "analyst"))

# COMMAND ----------

# DBTITLE 1,Trigger once stream from bronze to silver table - We can also stream FROM a Delta table!
dfSilver = spark.readStream.format("delta").load(bronzeTable)
dfSilver = dfSilver.drop("_rescued_data")

dfSilver.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", silverCheckpoint) \
  .start(silverTable)

# COMMAND ----------

dfSilver = spark.read.format("delta").load(silverTable)
display(dfSilver.orderBy("date", "stock_symbol", "analyst"))

# COMMAND ----------

# DBTITLE 1,Code used to build the 2nd EPS data set to import into the bronze table
df = spark.createDataFrame(
    [('3/1/2021','a',2,2.4),\
     ('4/1/2021','a',1,2.3),\
     ('4/1/2021','a',2,2.1),\
     ('4/1/2021','b',1,1.3),\
     ('4/1/2021','b',2,1.2),\
     ('4/1/2021','c',1,3.5),\
     ('4/1/2021','c',2,2.6)],
    ('date','stock_symbol','analyst','estimated_eps'))
# write one new csv file to the landing zone
df.repartition(1).write.mode("append").option("header","true").csv(landingZoneLocation)

# COMMAND ----------

# DBTITLE 1,Trigger once stream from autoloader to bronze table - Only new records are added
dfBronze = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", bronzeSchema) \
  .load(landingZoneLocation)

dfBronze.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", bronzeCheckpoint) \
  .start(bronzeTable)

# COMMAND ----------

dfBronze = spark.read.format("delta").load(bronzeTable)
display(dfBronze.orderBy("date", "stock_symbol", "analyst"))
