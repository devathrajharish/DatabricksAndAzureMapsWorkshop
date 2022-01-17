// Databricks notebook source
// MAGIC %md 
// MAGIC # Ingest Streaming Data from Azure Event Hub to Delta Lake
// MAGIC 
// MAGIC ### Overview
// MAGIC 
// MAGIC Azure Databricks is a data analytics platform optimized for the Microsoft Azure cloud services platform. Azure Databricks offers three environments for developing data intensive applications: Databricks SQL, Databricks Data Science & Engineering, and Databricks Machine Learning.
// MAGIC 
// MAGIC Azure Event Hubs is a big data streaming platform and event ingestion service. It can receive and process millions of events per second.Data sent to an event hub can be transformed and stored by using any real-time analytics provider or batching/storage adapters.
// MAGIC 
// MAGIC The following example will ingest data stream from Azure Eventhub directly to bronze Delta tables.We will send json messages to Eventhub and thereafter igest those messages in Azure Databricks and convert and store the data in Delta Lake
// MAGIC 
// MAGIC 
// MAGIC <img src="https://raw.githubusercontent.com/jodb/DatabricksAndAzureMapsWorkshop/master/episode4/azureDbIngestEtlArch.png">

// COMMAND ----------

// MAGIC %md
// MAGIC ##Install Libraries
// MAGIC 
// MAGIC 1. Add Event Hub connector - Ingest data from Eventhub https://github.com/Azure/azure-event-hubs-spark#databricks
// MAGIC 
// MAGIC 2. Send json messages - com.azure:azure-messaging-eventhubs:5.10.2
// MAGIC 
// MAGIC 3. Azure Databricks runtime version used for this example - DBR 8.3

// COMMAND ----------

// MAGIC %md
// MAGIC ## Let's create a JSON File to send to event hub

// COMMAND ----------

var json1 = """
{"name":"Tamy","age":22,"gpa":2.9}
"""
var json2 = """
{"name":"Ben","age":43,"gpa":3.9}
"""

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Publish Events to Azure Eventhub

// COMMAND ----------

// Store all secrets in Azure Key Vault and call them directly in Databricks notebook.
val namespaceName = "v-hdev-event"
val eventHubName = "v-hdev-hub"
val SharedAccessKeyName= "RootManageSharedAccessKey"
val sasKey = "<>"
val connStr = "Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<>"

// COMMAND ----------

// DBTITLE 1,Create function to send events
import scala.collection.JavaConverters._
import java.nio.charset.StandardCharsets.UTF_8
//import com.microsoft.azure.eventhubs._
import com.azure.messaging.eventhubs._

// create a producer using the namespace connection string and event hub name
val eventHubClient = new EventHubClientBuilder()
    .connectionString(connStr, eventHubName)
    .buildProducerClient();

def sleep(time: Long): Unit = Thread.sleep(time)
def sendEvent(message: String, delay: Long) = {
  sleep(delay)
  val batch = eventHubClient.createBatch();
  val messageData = new EventData(message)
  batch.tryAdd(messageData);
  eventHubClient.send(batch)
  System.out.println("Sent event: " + message + "\n")
}


// COMMAND ----------

// DBTITLE 1,Send events to Eventhub
sendEvent(json1,3)
sendEvent(json2,3)

// COMMAND ----------

// MAGIC %md
// MAGIC #Ingest Event Hub streams to Delta Lake

// COMMAND ----------

// MAGIC %python
// MAGIC # Pyspark and ML Imports
// MAGIC import time, os, json, requests
// MAGIC from pyspark.sql import functions as F
// MAGIC from pyspark.sql.types import * 
// MAGIC 
// MAGIC namespaceName = ["namespaceName"]
// MAGIC eventHubName = ["eventHubName"]
// MAGIC SharedAccessKeyName= ["SharedAccessKeyName"]
// MAGIC sasKey = ["sasKey"]
// MAGIC 
// MAGIC STREAM_ENDPOINT="Endpoint=sb://["namespaceName"].servicebus.windows.net/;SharedAccessKeyName["SharedAccessKeyName"];SharedAccessKey=["SharedAccessKeyName"];EntityPath=["eventHubName"]"
// MAGIC 
// MAGIC ehConf = { 
// MAGIC   'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(STREAM_ENDPOINT),
// MAGIC   'ehName': namespaceName
// MAGIC }
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC # Define schema for incoming data stream
// MAGIC schema = StructType([ 
// MAGIC     StructField("name",StringType(),True), 
// MAGIC     StructField("age",StringType(),True), 
// MAGIC     StructField("gpa",StringType(),True) 
// MAGIC   ])
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC # Read directly from IoT Hub using the EventHubs library for Databricks
// MAGIC iot_stream = (
// MAGIC   spark.readStream.format("eventhubs")                                               # Read from Event Hubs directly
// MAGIC     .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
// MAGIC     .load()                                                                          # Load the data
// MAGIC     .withColumn('readings', F.from_json(F.col('body').cast('string'), schema))       # Extract the "body" payload from the messages
// MAGIC     .select('readings.*' )                                                           
// MAGIC )
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC # Write To Delta Lake Bronze Layer
// MAGIC write_to_delta = (
// MAGIC  iot_stream                             
// MAGIC     .select('name','age','gpa') 
// MAGIC     .writeStream.format('delta')                                           # Write our stream to the Delta format
// MAGIC     .option("checkpointLocation","mnt/iot/checkpoint")                     # Checkpoint Location                                                          
// MAGIC     .start("/mnt/iot/bronze")                                              # Stream the data into an ADLS Path
// MAGIC )

// COMMAND ----------

// MAGIC %fs ls /mnt/iot/bronze/

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS iot_demo

// COMMAND ----------

// DBTITLE 1,Create Delta Table - Bronze
// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS iot_demo.sample USING DELTA LOCATION '/mnt/iot/bronze/'

// COMMAND ----------

// DBTITLE 1,Let's look at the stream data !!
// MAGIC %sql
// MAGIC select * from iot_demo.sample

// COMMAND ----------

// MAGIC %md
// MAGIC ###Further Reference
// MAGIC 1.Below is the link to a two Part series which shocases end-to-end ingestion of IOT data from Event/IOT hub to Azure Databricks.
// MAGIC [Part 1](https://databricks.com/blog/2020/08/03/modern-industrial-iot-analytics-on-azure-part-1.html)
// MAGIC [Part 2](https://databricks.com/blog/2020/08/11/modern-industrial-iot-analytics-on-azure-part-2.html)
// MAGIC 
// MAGIC 2.Another great tutorial that explains how to ingest data from Event Hub into Azure Databricks [Blog](https://docs.microsoft.com/en-us/azure/databricks/scenarios/databricks-stream-from-eventhubs)
