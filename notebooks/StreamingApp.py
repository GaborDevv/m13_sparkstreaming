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

spark.sql("set spark.sql.streaming.schemaInference=true")

# COMMAND ----------

hotel_df = spark.readStream.format(sourceFileFormat) \
.option("header", "true") \
.option("inferSchema", "true") \
.load(file_location+"hotel-weather")

# COMMAND ----------

hotel_df = hotel_df.withColumn("wthr_date", col("wthr_date").cast(DateType())).withColumn("TimeStamp", current_date())

# COMMAND ----------

aggregated_hotel_df = hotel_df.groupBy("city",  "wthr_date").agg(
    approxCountDistinct("id").alias("distinct_hotel_ids"),
    avg("avg_tmpr_c").alias("average_temperature_in_c"),
    max("avg_tmpr_c").alias("max_temperature_in_c"),
    min("avg_tmpr_c").alias("min_temperature_in_c")
)
aggregated_hotel_df = aggregated_hotel_df.withColumn("TimeStamp", current_timestamp()).withWatermark("TimeStamp", "1 minute")

# COMMAND ----------

top_ten_df = hotel_df.groupBy("city").agg(approxCountDistinct("id").alias("distinct_hotels")).orderBy(col("distinct_hotels").desc()).limit(10).drop("distinct_hotels")

# COMMAND ----------

display(top_ten_df)

# COMMAND ----------

top_query = (
    top_ten_df.writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("top_ten_stream")
    .start()
)

# COMMAND ----------

aggregated_hotel_df.createOrReplaceTempView("hotel_data")
top_ten_df.createOrReplaceTempView("top_ten")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hotel_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT h.wthr_date, h.distinct_hotel_ids, h.average_temperature_in_c, h.max_temperature_in_c, h.min_temperature_in_c, t.city FROM top_ten_stream t JOIN hotel_data h ON t.city = h.city

# COMMAND ----------

df = _sqldf
df.createOrReplaceTempView("dataview")

# COMMAND ----------

display(df)

# COMMAND ----------

lists = []
def process_batch(batch_df, batch_id):
    # Collect data from the current micro-batch and append it to the list
    snapshot = batch_df.select("city").collect()
    lists.append(snapshot)
query = (
    top_ten_df
    .writeStream
    .outputMode("complete")
    .foreachBatch(process_batch)
    .start()
)

# COMMAND ----------

print(city_names)

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


