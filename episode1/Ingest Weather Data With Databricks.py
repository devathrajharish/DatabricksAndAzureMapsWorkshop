# Databricks notebook source
# MAGIC %md
# MAGIC #Modern analytics architecture with Azure Databricks
# MAGIC <img src="https://docs.microsoft.com/en-us/azure/architecture/solution-ideas/media/azure-databricks-modern-analytics-architecture-diagram.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC #The Delta Lake Architecture
# MAGIC 
# MAGIC <img src="https://docs.microsoft.com/en-us/learn/databricks/describe-azure-databricks-delta-lake-architecture/media/delta-azure.png" width=800>
# MAGIC 
# MAGIC **Key Features Of Delta Lake**
# MAGIC * **ACID transactions on Delta Lake:** Serializable isolation levels ensure that readers never see inconsistent data.
# MAGIC * **Streaming and batch unification:** A table in Delta Lake is a batch table as well as a streaming source and sink. Streaming data ingest, batch historic backfill, interactive queries all just work out of the box.
# MAGIC * **Schema enforcement:** Automatically handles schema variations to prevent insertion of bad records during ingestion.
# MAGIC * **Upserts and deletes:** Supports merge, update and delete operations to enable complex use cases like change-data-capture, slowly-changing-dimension (SCD) operations, streaming upserts, and so on.
# MAGIC 
# MAGIC **Databricks, Delta Lake and ETL References**
# MAGIC * [Modern analytics architecture with Azure Databricks](https://docs.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture)
# MAGIC * [Ingestion, ETL, and stream processing pipelines with Azure Databricks](https://docs.microsoft.com/en-us/azure/architecture/solution-ideas/articles/ingest-etl-stream-with-adb)
# MAGIC * [The Delta Lake Guide](https://docs.microsoft.com/en-us/azure/databricks/delta/)
# MAGIC * [COPY INTO](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/delta-copy-into)
# MAGIC * [Auto Loader](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader)
# MAGIC * [This Azure module is a great way to learn about Delta Lake](https://docs.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture)
# MAGIC <br />

# COMMAND ----------

# MAGIC %md
# MAGIC #Azure Open Datasets
# MAGIC 
# MAGIC Azure has opened up many datasets for use, here are a couple links to the data available to you!
# MAGIC * [Azure Open Datasets](https://docs.microsoft.com/en-us/azure/open-datasets/dataset-catalog)
# MAGIC * [Azure ML Opendatasets Package](https://docs.microsoft.com/en-us/python/api/azureml-opendatasets/azureml.opendatasets?view=azure-ml-py)
# MAGIC 
# MAGIC Today, we are specifically going to ingest weather from the [NoaaIsdWeather](https://docs.microsoft.com/en-us/python/api/azureml-opendatasets/azureml.opendatasets.noaaisdweather?view=azure-ml-py) dataset.
# MAGIC 
# MAGIC Weather data, as well as the other datasets available, are extremely useful in all kinds different practical applications! [Here is a use case](https://customers.microsoft.com/en-us/story/1378282338316029794-open-grid-europe-azure-en) where OGE is using weather data, along with other date sets, to monitor pipelines using AI. And [another use case](https://customers.microsoft.com/en-us/story/1383939746880579643-accuweather-partner-professional-services-azure-databricks) from AccuWeather on, well, predicting the weather:)
# MAGIC 
# MAGIC #Happy hacking and we can't wait to see what you are able to develop!

# COMMAND ----------


