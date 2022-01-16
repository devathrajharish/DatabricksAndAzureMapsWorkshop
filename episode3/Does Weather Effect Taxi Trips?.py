# Databricks notebook source
# MAGIC %md
# MAGIC ## Does Weather Effect Taxi Trips?
# MAGIC 
# MAGIC We now have two data sets, historical weather data and taxi data on trips. Let's take the two sets and determine if rain and snow effect taxi fairs on a daily basis. We will use machine learning to determine if weather effects taxi fairs but to do that, we first need to understand what data we have and how to prepare it. Let's dig in!
# MAGIC 
# MAGIC <img src="https://camo.githubusercontent.com/8ba8aa43f426056299acefccb14719ecfe4b123f18d5845474564dfabe4628ef/68747470733a2f2f696d6167652e736c696465736861726563646e2e636f6d2f3462726f6f6b6577656e69676a756c657364616d6a692d3138303631323232313334322f39352f612d74616c652d6f662d74687265652d646565702d6c6561726e696e672d6672616d65776f726b732d74656e736f72666c6f772d6b657261732d616e642d646565702d6c6561726e696e672d706970656c696e65732d776974682d62726f6f6b652d77656e69672d616e642d6a756c65732d64616d6a692d352d3633382e6a70673f63623d31353238383431363939" width=800>

# COMMAND ----------

# DBTITLE 1,Install libraries
# MAGIC %pip install azureml-opendatasets

# COMMAND ----------

# DBTITLE 1,Import libraries and set configurations
from azureml.opendatasets import NoaaIsdWeather
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

from pyspark.sql.functions import *

username = spark.sql("select current_user()").collect()[0][0]
userPrefix = username.split("@")[0].replace(".", "")
basePath = "/tmp/" + username + "/taxiData"
weatherStationsTableLocation = basePath + "/datastore/weatherStations" 
dailyWeatherTableLocation = basePath + "/datastore/dailyWeather" 
lgaWeatherTableLocation = basePath + "/datastore/lgaDailyWeather"
dailyTripsTableLocation = basePath + "/datastore/dailyTrips"
dailyFeaturesTableLocation = basePath + "/datastore/dailyFeatures"
spark.conf.set("c.weatherStationsDataPath", "dbfs:" + weatherStationsTableLocation)
spark.conf.set("c.dailyTripsDataPath", "dbfs:" + dailyTripsTableLocation)

# COMMAND ----------

# DBTITLE 1,Read weather data from the NOAA library
start_date = parser.parse('2016-07-01')
end_date = parser.parse('2019-12-31')

isd = NoaaIsdWeather(start_date, end_date)
df = isd.to_spark_dataframe()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Where are all the weather stations?
dfStations = df.groupBy(df.stationName, df.latitude, df.longitude).count()
dfStations.write.format("delta").mode("overwrite").save(weatherStationsTableLocation)
display(dfStations)

# COMMAND ----------

# DBTITLE 1,Add weather stations table to view the data in Databricks SQL
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS taxi.weatherStations;
# MAGIC 
# MAGIC CREATE TABLE taxi.weatherStations
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.weatherStationsDataPath}';
# MAGIC 
# MAGIC USE taxi;
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 1,Aggregate accumulation by day
df = df.withColumn("date", to_date(df.datetime))
dfAgg = df.groupBy(df.stationName, df.date, df.year, df.month, df.day)\
  .agg(max(df.precipDepth).alias("maxRain"), sum(df.precipDepth).alias("sumRain"), max(df.snowDepth).alias("maxSnow"), sum(df.snowDepth).alias("sumSnow"))


# COMMAND ----------

# DBTITLE 1,Only need to keep weather data for the La Guardia station
dfLga = dfAgg.where(dfAgg.stationName == "LA GUARDIA AIRPORT").orderBy("date")
dfLga.write.format("delta").mode("overwrite").save(lgaWeatherTableLocation)
display(dfLga)

# COMMAND ----------

# DBTITLE 1,Load taxi data from the Databricks datasets
# MAGIC %fs ls /databricks-datasets/nyctaxi/tripdata/

# COMMAND ----------

# DBTITLE 1,All green taxi historical data
dfTripsGreen = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv("/databricks-datasets/nyctaxi/tripdata/green/")

display(dfTripsGreen)

# COMMAND ----------

# DBTITLE 1,Aggregate taxi trip data by day
dfTrips = dfTripsGreen.withColumn("pickupDate", to_date(dfTripsGreen.lpep_pickup_datetime))
dfTrips = dfTrips.where(dfTrips.pickupDate.between("2016-07-01", "2019-12-31"))

dfDailyTrips = dfTrips.groupBy(dfTrips.pickupDate)\
  .agg(sum(dfTrips.Trip_distance).alias("sumDistance"), sum(dfTrips.Total_amount).alias("sumTotalFare"), count(dfTrips.Total_amount).alias("tripCount"))\
  .orderBy(dfTrips.pickupDate)

dfDailyTrips.write.format("delta").mode("overwrite").save(dailyTripsTableLocation)

display(dfDailyTrips)

# COMMAND ----------

# DBTITLE 1,Add daily trips table to view the data in Databricks SQL
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS taxi.dailyTrips;
# MAGIC 
# MAGIC CREATE TABLE taxi.dailyTrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.dailyTripsDataPath}';
# MAGIC 
# MAGIC USE taxi;
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 1,Combine data sets into an ml features data set
dfDailyTrips = spark.read.format("delta").load(dailyTripsTableLocation)

dfLga = spark.read.format("delta").load(lgaWeatherTableLocation)

dfDailyFeatures = dfLga.join(dfDailyTrips,  dfDailyTrips.pickupDate == dfLga.date)
dfDailyFeatures = dfDailyFeatures.drop("pickupdate")
dfDailyFeatures = dfDailyFeatures.withColumn("dayofweek", dayofweek(dfDailyFeatures.date))

dfDailyFeatures.write.format("delta").mode("overwrite").save(dailyFeaturesTableLocation)

display(dfDailyFeatures)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gradient Boosting Trees (GBT) and Feature Importance
# MAGIC GBT and other tree based models return feature importance and we will use these importance values to prove weater effects taxi trips

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer, StringIndexer

dfFeature = spark.read.format("delta").load(dailyFeaturesTableLocation)
dfFeature = dfFeature.withColumn("sumAccumulation", dfFeature.sumRain + dfFeature.sumSnow)

# Setting variables to predict bad loans
numericCols = ["month", "day", "sumAccumulation"]
imputers = Imputer(inputCols = numericCols, outputCols = numericCols)

# Add day of week as an indexed feature
dowIndexer = StringIndexer(inputCol="dayofweek", outputCol="dayofweek_idx")
featureCols = numericCols + ["dayofweek_idx"]
vectorAssembler = VectorAssembler(inputCols=featureCols, outputCol="features")

# Define a GBT model.
gbt = GBTRegressor(featuresCol="features",
                    labelCol="sumTotalFare")

# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=[imputers, dowIndexer, vectorAssembler, gbt])

# Train model.  This also runs the indexer.
gbt_model = pipeline.fit(dfFeature)

# COMMAND ----------

# DBTITLE 1,It looks like accumulation (rain and snow) is the 2nd most important feature!
import pandas as pd
import numpy as np

featureImportances = gbt_model.stages[3].featureImportances
df = pd.DataFrame([featureImportances.toArray(), np.array(featureCols)]).transpose()
df.columns =["Feature Importance", "Feature"]
display(df)

# COMMAND ----------


