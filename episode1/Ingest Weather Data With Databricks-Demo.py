# Databricks notebook source
# MAGIC %md
# MAGIC #MSFT Dev Radio Series - Part 1 - Data Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing Your Enterprise Data Lake
# MAGIC Databricks enables an architecture where your analytics is decoupled from your data storage. This allows organizations to store their data cost effectively in Azure Storage and share their data across best of breed tools in Azure without duplicating it in data silos. 
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/Delta%20Lakehouse.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Ingestion Architecture - Delta Lake
# MAGIC <img src="https://kpistoropen.blob.core.windows.net/collateral/quickstart/etl.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Let's Begin Ingesting Some Data

# COMMAND ----------

# DBTITLE 1,Install Libraries
pip install azureml-opendatasets

# COMMAND ----------

from azureml.opendatasets import NoaaIsdWeather
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### For this Demo we are storing the data in [dbfs](https://docs.databricks.com/data/databricks-file-system.html) (Databricks File System). This is a default blob storage that comes with Databricks Workspace. 
# MAGIC #### One should use ADLS Gen2 or Blob storage to store all data and process data from there.

# COMMAND ----------

# remove the database and also the base folder if it already exists
#spark.sql("DROP TABLE copyIntoTable")
spark.sql("DROP DATABASE if exists ingest_bronze cascade")
dbutils.fs.rm("/tmp/ingestGettingStartedDemo", recurse = True)

# COMMAND ----------

# Create Landing Zone Folders
basePath = "/mnt/tutorial/ingestGettingStartedDemo"
#landingZoneLocation = basePath + "/landingZone"     
try:
  dbutils.fs.ls(basePath)
except:
  dbutils.fs.mkdirs(basePath)
  #dbutils.fs.mkdirs(landingZoneLocation)
else:
  raise Exception("The folder " + basePath + " already exists, this notebook will remove it at the end, please change the basePath or remove the folder first")

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/tutorial/ingestGettingStartedDemo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now we are ready to read data from the NOAA Library

# COMMAND ----------

# DBTITLE 1,Let's start with only 10 days worth of data for May 2020

start_date = parser.parse('2020-5-1')
end_date = parser.parse('2020-5-10')

isd = NoaaIsdWeather(start_date, end_date)
pdf = isd.to_spark_dataframe().toPandas().to_csv("/dbfs/mnt/tutorial/ingestGettingStartedDemo/May_2020_pandas.csv")

# COMMAND ----------

# DBTITLE 1,Checking that the raw data landed in the folder
# MAGIC %fs ls dbfs:/mnt/tutorial/ingestGettingStartedDemo/

# COMMAND ----------

# MAGIC %md
# MAGIC ##Auto Loader, COPY INTO and Incrementally Ingesting Data
# MAGIC Auto Loader and COPY INTO are two methods of ingesting data into a Delta Lake table from a folder in a Data Lake. “Yeah, so... Why is that so special?”, you may ask. The reason these features are special is that they make it possible to ingest data directly from a data lake incrementally, in an idempotent way, without needing a distributed streaming system like Kafka. This can considerably simplify the Incremental ETL process. It is also an extremely efficient way to ingest data since you are only ingesting new data and not reprocessing data that already exists. Below is an Incremental ETL architecture. We will focus on the left hand side, ingesting into tables from outside sources. 
# MAGIC 
# MAGIC You can incrementally ingest data either continuously or scheduled in a job. COPY INTO and Auto Loader cover both cases and we will show you how below.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/get-start-delta-blog-img-1.png">

# COMMAND ----------

# MAGIC %md
# MAGIC #COPY INTO

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ingest_bronze LOCATION 'dbfs:/mnt/tutorial/copy_into/copyfolder/user';
# MAGIC USE ingest_bronze;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO delta.`dbfs:/mnt/tutorial/copy_into/copyfolder/user`   FROM 'dbfs:/mnt/tutorial/ingestGettingStartedDemo/'   FILEFORMAT = CSV  FORMAT_OPTIONS('header'='true');

# COMMAND ----------

# DBTITLE 1,View the Delta Files
# MAGIC %fs ls dbfs:/mnt/tutorial/copy_into/copyfolder/user

# COMMAND ----------

# DBTITLE 1,Create a Table "weather"
# MAGIC %sql
# MAGIC CREATE TABLE weather USING DELTA LOCATION 'dbfs:/mnt/tutorial/copy_into/copyfolder/user';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather

# COMMAND ----------

# MAGIC %md
# MAGIC ##Auto Loader
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) provides a method to stream in new data from a folder location into a Delta Lake table by using one of two methods.  The directory listing method monitors the files in a directory and identifies new files or files that have been changed since last time new data was processed.  This method is the default method and is preferred when file folders have a smaller number of files in it.  For other scenarios, the file notification method sends a notification when a new file appears or is changed. 
# MAGIC 
# MAGIC 
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) provides python and scala methods to ingest new data from a folder location into a Delta Lake table by using directory listing or file notifications. While Auto Loader is a Structured Streaming source, it does not have to run continuously. You can use the [trigger once](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html) option to turn it into a job that turns itself off, which we do below.  The directory listing method monitors the files in a directory and identifies new files or files that have been changed since last time new data was processed.  This method is the default method and is preferred when file folders have a smaller number of files in it.  For other scenarios, the file notification method relies on the cloud service to send a notification when a new file appears or is changed. 

# COMMAND ----------

# autoloader table and checkpoint paths
autoloaderTable = "dbfs:/mnt/tutorial/ingestAutoloader/autoloader"
autoloaderCheckpoint = "dbfs:/mnt/tutorial/ingestAutoloader/autoloader/checkpoint"
landingZoneLocation = "dbfs:/mnt/tutorial/ingestGettingStartedDemo/"

# COMMAND ----------

# DBTITLE 1,Python - Using autoloader, copy the same data into a different Delta table
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", autoloaderCheckpoint) \
  .load(landingZoneLocation)\
  .writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", autoloaderCheckpoint) \
  .start(autoloaderTable)

# COMMAND ----------

# DBTITLE 1,Wait for the above cell to finish running before running this one
dfAutoloader = spark.read.format("delta").load(autoloaderTable)
display(dfAutoloader)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/tutorial/ingestAutoloader/autoloader/

# COMMAND ----------

# MAGIC %md
# MAGIC ##Below we will write a second file to the landing zone and only new files will be ingested

# COMMAND ----------

# DBTITLE 1,Let's continue and ingest  10 days worth of data for June 2020
start_date = parser.parse('2020-6-1')
end_date = parser.parse('2020-6-10')
isd = NoaaIsdWeather(start_date, end_date)
pdf = isd.to_spark_dataframe().toPandas().to_csv("/dbfs/mnt/tutorial/ingestGettingStartedDemo/June_2020_pandas.csv")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/tutorial/ingestGettingStartedDemo/

# COMMAND ----------

# DBTITLE 1,SQL - Copy data from the same location into the table
# MAGIC %sql
# MAGIC USE ingest_bronze;
# MAGIC 
# MAGIC COPY INTO delta.`dbfs:/mnt/tutorial/copy_into/copyfolder/user`   FROM 'dbfs:/mnt/tutorial/ingestGettingStartedDemo/'   FILEFORMAT = CSV  FORMAT_OPTIONS('header'='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from weather

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesting the same file using Autoloader

# COMMAND ----------

# DBTITLE 1,Python - Autoloader, copy data into a table
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", autoloaderCheckpoint) \
  .load(landingZoneLocation)\
  .writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", autoloaderCheckpoint) \
  .start(autoloaderTable)

# COMMAND ----------

# DBTITLE 1,Wait for the above cell to finish running before running this one
dfAutoloader = spark.read.format("delta").load(autoloaderTable)
display(dfAutoloader)

# COMMAND ----------


