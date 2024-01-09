# Databricks notebook source
import pyspark.sql
from pyspark.sql.functions import col, approxCountDistinct, avg, max, min, current_date,current_timestamp, row_number

from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md
# MAGIC Connecting to Gen2 datalake storage

# COMMAND ----------

storage_account_name = "stagabordevvwesteurope"
storage_account_access_key = "/r0LI2y9zfyy1IoP9vlM3peeqSNW+vMEUmsWaGlN2gnsr8ZzU4d2FKNTouVR4X2sXfa5CyDdD1Qx+AStrutVoQ=="
file_location = f"abfss://sparkstreamingdata@{storage_account_name}.dfs.core.windows.net/"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_access_key,
)
sourceFileFormat = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC Spark config to infer the schema of the files

# COMMAND ----------

spark.sql("set spark.sql.streaming.schemaInference=true")

# COMMAND ----------

# MAGIC %md
# MAGIC read stream of data (Do I need to make more configurations?)

# COMMAND ----------

hotel_df = spark.readStream.format(sourceFileFormat) \
.option("header", "true") \
.option("inferSchema", "true") \
.load(file_location+"hotel-weather")

# COMMAND ----------

# MAGIC %md
# MAGIC Adding column called Timestamp and casting weather_date col to Date type 

# COMMAND ----------

hotel_df = hotel_df.withColumn("wthr_date", col("wthr_date").cast(DateType())).withColumn("TimeStamp", current_date())

# COMMAND ----------

# MAGIC %md
# MAGIC creating aggregated hotel data: in every city on each day: number of hotels and the corresponding weather data(average, minimum, maximum)

# COMMAND ----------

aggregated_hotel_df = hotel_df.groupBy("city",  "wthr_date").agg(
    approxCountDistinct("id").alias("distinct_hotel_ids"),
    avg("avg_tmpr_c").alias("average_temperature_in_c"),
    max("avg_tmpr_c").alias("max_temperature_in_c"),
    min("avg_tmpr_c").alias("min_temperature_in_c")
)
aggregated_hotel_df = aggregated_hotel_df.withColumn("TimeStamp", current_timestamp()).withWatermark("TimeStamp", "1 minute")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating helper table which finds the top 10 biggest cities(giving home to most hotels)

# COMMAND ----------

top_ten_df = hotel_df.groupBy("city").agg(approxCountDistinct("id").alias("distinct_hotels")).orderBy(col("distinct_hotels").desc()).limit(10).drop("distinct_hotels")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating query for joining and this way filtering aggregated hotel data(join it with the top 10 cities)

# COMMAND ----------

top_query = (
    top_ten_df.writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("top_ten_stream")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC create temprorary view tables

# COMMAND ----------

aggregated_hotel_df.createOrReplaceTempView("hotel_data")
top_ten_df.createOrReplaceTempView("top_ten")

# COMMAND ----------

# MAGIC %md
# MAGIC making the aforementioned join

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT h.wthr_date, h.distinct_hotel_ids, h.average_temperature_in_c, h.max_temperature_in_c, h.min_temperature_in_c, t.city FROM top_ten_stream t JOIN hotel_data h ON t.city = h.city

# COMMAND ----------

# MAGIC %md
# MAGIC Saving the result of the query to a dataframe

# COMMAND ----------

df = _sqldf

# COMMAND ----------

# MAGIC %md
# MAGIC Create a list of the top 10 cities to make iterative query possible

# COMMAND ----------

temp_list = []
def process_batch(batch_df, batch_id):
    # Collect data from the current micro-batch and append it to the list
    snapshot = batch_df.select("city").collect()
    temp_list.append(snapshot)
query = (
    top_ten_df
    .writeStream
    .outputMode("complete")
    .foreachBatch(process_batch)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC displaying and visualizing frames one-by-one because otherwise display overwrites the table and only one is displayed at a time
# MAGIC

# COMMAND ----------

city_names = [row.city for row in lists[0]]
city_dataframes = []
for city in city_names:
    # Filter the DataFrame for the current city
    df_city = df.filter(col("city") == city)
    # Display the chart for the current city
    city_dataframes.append(df_city)
display(city_dataframes[0])

# COMMAND ----------

display(city_dataframes[1])

# COMMAND ----------

display(city_dataframes[2])

# COMMAND ----------

display(city_dataframes[3])

# COMMAND ----------

display(city_dataframes[4])

# COMMAND ----------

display(city_dataframes[5])

# COMMAND ----------

display(city_dataframes[6])

# COMMAND ----------

display(city_dataframes[7])

# COMMAND ----------

display(city_dataframes[8])

# COMMAND ----------

display(city_dataframes[9])

# COMMAND ----------


