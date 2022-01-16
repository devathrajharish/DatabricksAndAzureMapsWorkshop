# Databricks notebook source
# MAGIC %md
# MAGIC #Databricks SQL - SQL Analytics on your Lakehouse
# MAGIC 
# MAGIC This notebook includes two sections. The first is ingesting and lightly transforming [NYC taxi data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and the second is SQL queries that we will use in [Databrikcs SQL](https://docs.microsoft.com/en-us/azure/databricks/scenarios/sql/). In the architecture below, you can think of the raw taxi data as the ingest on the left side, the transformation into a Delta Lake table as the middle, and Databricks SQL queries as BI on the right. For the taxi data, we will ingest it from the **databricks-datasets** folder accessible from all workspaces but you can also use [Azure Open Datasets](https://docs.microsoft.com/en-us/azure/open-datasets/dataset-catalog) or [Azure ML Opendatasets Package](https://docs.microsoft.com/en-us/python/api/azureml-opendatasets/azureml.opendatasets?view=azure-ml-py) as we did in our first episode.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/get-start-delta-blog-img-1.png" width=800>

# COMMAND ----------

# DBTITLE 1,Set Configurations
username = spark.sql("select current_user()").collect()[0][0]
userPrefix = username.split("@")[0].replace(".", "")
basePath = "/mnt/tutorial" + username + "/taxiData"
schemaLocation = basePath + "/schemaStore"
tableLocation = basePath + "/datastore/taxi" 
checkPointLocation = basePath + "/datastore/taxiCheckpoint"

spark.conf.set("c.TaxiDataPath", "dbfs:" + tableLocation)

# COMMAND ----------

# MAGIC %md
# MAGIC #NYC Taxi Ingest and Transformation

# COMMAND ----------

# DBTITLE 1,NYC Taxi Dataset in databricks-datasets that we will use
# MAGIC %fs ls /databricks-datasets/nyctaxi/tripdata/green/

# COMMAND ----------

# DBTITLE 1,Ingest the needed data and transform it a little
# Read one month of taxi data
df = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv("/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2015-12.csv.gz")

# update timestamp columns that came in as strings
df = df.withColumn("lpep_pickup_datetime", df.lpep_pickup_datetime.cast("timestamp"))\
  .withColumn("Lpep_dropoff_datetime", df.Lpep_dropoff_datetime.cast("timestamp"))

# for convience of the demo, restrict the data set a bit
df = df.where(df.lpep_pickup_datetime.between("2015-12-01T00:00:00.000","2015-12-08T00:00:00.000") \
             & (df.Trip_distance > 0) \
             & (df.Fare_amount > 0) \
             & (df.RateCodeID == 5))

# overwrite the current Delta Lake table if it exists
df.write.format("delta").mode("overwrite").save(tableLocation)

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Set up the Delta Lake Table used in SQL
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS taxi.taxi_trips;
# MAGIC 
# MAGIC CREATE TABLE taxi.taxi_trips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.TaxiDataPath}';

# COMMAND ----------

# MAGIC %sql
# MAGIC desc taxi.taxi_trips

# COMMAND ----------

# MAGIC %md
# MAGIC #NYC Taxi Dashboard SQL Queries

# COMMAND ----------

# DBTITLE 1, Daily Fare to Distance Analysis
# MAGIC %sql
# MAGIC SELECT
# MAGIC   T.weekday,
# MAGIC   CASE
# MAGIC     WHEN T.weekday = 1 THEN 'Sunday'
# MAGIC     WHEN T.weekday = 2 THEN 'Monday'
# MAGIC     WHEN T.weekday = 3 THEN 'Tuesday'
# MAGIC     WHEN T.weekday = 4 THEN 'Wednesday'
# MAGIC     WHEN T.weekday = 5 THEN 'Thursday'
# MAGIC     WHEN T.weekday = 6 THEN 'Friday'
# MAGIC     WHEN T.weekday = 7 THEN 'Saturday'
# MAGIC     ELSE 'N/A'
# MAGIC   END AS day_of_week, 
# MAGIC   T.fare_amount, 
# MAGIC   T.trip_distance
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       dayofweek(lpep_pickup_datetime) as weekday,
# MAGIC       *
# MAGIC     FROM
# MAGIC       taxi.taxi_trips
# MAGIC   ) T
# MAGIC ORDER BY
# MAGIC   T.weekday  

# COMMAND ----------

# DBTITLE 1,Mapped Pick Up Locations
# MAGIC %sql
# MAGIC SELECT Pickup_longitude, Pickup_latitude
# MAGIC FROM taxi.taxi_trips
# MAGIC WHERE Pickup_longitude != 0 OR Pickup_latitude != 0

# COMMAND ----------

# DBTITLE 1,Total Trips
# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_trips
# MAGIC FROM taxi.taxi_trips

# COMMAND ----------

# DBTITLE 1,Total Fair Amount
# MAGIC %sql
# MAGIC SELECT SUM(Total_amount) as total_fair_amount
# MAGIC FROM taxi.taxi_trips

# COMMAND ----------

# DBTITLE 1,Pickup Hour Distribution
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN T.pickup_hour = 0 THEN '00:00'
# MAGIC         WHEN T.pickup_hour = 1 THEN '01:00'
# MAGIC         WHEN T.pickup_hour = 2 THEN '02:00'
# MAGIC         WHEN T.pickup_hour = 3 THEN '03:00'
# MAGIC         WHEN T.pickup_hour = 4 THEN '04:00'
# MAGIC         WHEN T.pickup_hour = 5 THEN '05:00'
# MAGIC         WHEN T.pickup_hour = 6 THEN '06:00'
# MAGIC         WHEN T.pickup_hour = 7 THEN '07:00'
# MAGIC         WHEN T.pickup_hour = 8 THEN '08:00'
# MAGIC         WHEN T.pickup_hour = 9 THEN '09:00'
# MAGIC         WHEN T.pickup_hour = 10 THEN '10:00'
# MAGIC         WHEN T.pickup_hour = 11 THEN '11:00'
# MAGIC         WHEN T.pickup_hour = 12 THEN '12:00'
# MAGIC         WHEN T.pickup_hour = 13 THEN '13:00'
# MAGIC         WHEN T.pickup_hour = 14 THEN '14:00'
# MAGIC         WHEN T.pickup_hour = 15 THEN '15:00'
# MAGIC         WHEN T.pickup_hour = 16 THEN '16:00'
# MAGIC         WHEN T.pickup_hour = 17 THEN '17:00'
# MAGIC         WHEN T.pickup_hour = 18 THEN '18:00'
# MAGIC         WHEN T.pickup_hour = 19 THEN '19:00'
# MAGIC         WHEN T.pickup_hour = 20 THEN '20:00'
# MAGIC         WHEN T.pickup_hour = 21 THEN '21:00'
# MAGIC         WHEN T.pickup_hour = 22 THEN '22:00'
# MAGIC         WHEN T.pickup_hour = 23 THEN '23:00'
# MAGIC     ELSE 'N/A'
# MAGIC     END AS `Pickup Hour`,
# MAGIC     T.num AS `Number of Rides`
# MAGIC FROM
# MAGIC (
# MAGIC SELECT 
# MAGIC   hour(lpep_pickup_datetime) AS pickup_hour,
# MAGIC   COUNT(*) AS num
# MAGIC FROM
# MAGIC   taxi.taxi_trips
# MAGIC GROUP BY hour(lpep_pickup_datetime)
# MAGIC ORDER BY hour(lpep_pickup_datetime)
# MAGIC ) T

# COMMAND ----------

# DBTITLE 1,Dropoff Hour Distribution
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN T.dropoff_hour = 0 THEN '00:00'
# MAGIC         WHEN T.dropoff_hour = 1 THEN '01:00'
# MAGIC         WHEN T.dropoff_hour = 2 THEN '02:00'
# MAGIC         WHEN T.dropoff_hour = 3 THEN '03:00'
# MAGIC         WHEN T.dropoff_hour = 4 THEN '04:00'
# MAGIC         WHEN T.dropoff_hour = 5 THEN '05:00'
# MAGIC         WHEN T.dropoff_hour = 6 THEN '06:00'
# MAGIC         WHEN T.dropoff_hour = 7 THEN '07:00'
# MAGIC         WHEN T.dropoff_hour = 8 THEN '08:00'
# MAGIC         WHEN T.dropoff_hour = 9 THEN '09:00'
# MAGIC         WHEN T.dropoff_hour = 10 THEN '10:00'
# MAGIC         WHEN T.dropoff_hour = 11 THEN '11:00'
# MAGIC         WHEN T.dropoff_hour = 12 THEN '12:00'
# MAGIC         WHEN T.dropoff_hour = 13 THEN '13:00'
# MAGIC         WHEN T.dropoff_hour = 14 THEN '14:00'
# MAGIC         WHEN T.dropoff_hour = 15 THEN '15:00'
# MAGIC         WHEN T.dropoff_hour = 16 THEN '16:00'
# MAGIC         WHEN T.dropoff_hour = 17 THEN '17:00'
# MAGIC         WHEN T.dropoff_hour = 18 THEN '18:00'
# MAGIC         WHEN T.dropoff_hour = 19 THEN '19:00'
# MAGIC         WHEN T.dropoff_hour = 20 THEN '20:00'
# MAGIC         WHEN T.dropoff_hour = 21 THEN '21:00'
# MAGIC         WHEN T.dropoff_hour = 22 THEN '22:00'
# MAGIC         WHEN T.dropoff_hour = 23 THEN '23:00'
# MAGIC     ELSE 'N/A'
# MAGIC     END AS `Dropoff Hour`,
# MAGIC     T.num AS `Number of Rides`
# MAGIC FROM
# MAGIC (SELECT
# MAGIC   hour(Lpep_dropoff_datetime) AS dropoff_hour,
# MAGIC   COUNT(*) AS num
# MAGIC FROM
# MAGIC   taxi.taxi_trips
# MAGIC GROUP BY hour(Lpep_dropoff_datetime)
# MAGIC ORDER BY hour(Lpep_dropoff_datetime)
# MAGIC ) T

# COMMAND ----------


