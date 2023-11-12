"""
This script reads a CSV file from a specified location, performs some data transformations, and writes the processed data to a specified location. The transformations include converting a date column to dates, window transformations, and casting columns to appropriate types. The processed data is written to a CSV file. 

The input file location, output file location, file type, infer schema, first row is header, and delimiter are configurable.

The script assumes that the input file exists and the output file does not exist. If the output file exists, it will be deleted before writing the processed data.

The script requires the following libraries: os, pyspark.sql.SparkSession, pyspark.sql.functions, and pyspark.sql.window.
"""
# Databricks notebook source
# MAGIC %md Library Imports
# MAGIC

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofmonth, month, year
from pyspark.sql.functions import date_format, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md Spark Session Creation
# MAGIC

# COMMAND ----------

spark = SparkSession.builder.appName("dataeng_week10").getOrCreate()

# COMMAND ----------

# MAGIC %md Configurations
# MAGIC

# COMMAND ----------

INPUT_FILE_LOCATION = "/FileStore/tables/lacity_org_website_traffic.csv"
OUTPUT_FILE_LOCATION = "/FileStore/tables/processed_lacity_org_website_traffic.csv"
FILE_TYPE = "csv"
INFER_SCHEMA = "false"
FIRST_ROW_IS_HEADER = "true"
DELIMITER = ","


# COMMAND ----------

# Read the data
raw_data = (
    spark.read.format(FILE_TYPE)
    .option("inferSchema", INFER_SCHEMA)
    .option("header", FIRST_ROW_IS_HEADER)
    .option("sep", DELIMITER)
    .load(INPUT_FILE_LOCATION)
)

# COMMAND ----------

# MAGIC %md Data Transformation
# MAGIC

# COMMAND ----------

# Convert date to dates
raw_data = raw_data.withColumn("timestamp", col("Date")).drop("Date")
raw_data = raw_data.withColumn("date", to_date(col("timestamp")))
raw_data = raw_data.withColumn("day", dayofmonth(col("date")))
raw_data = raw_data.withColumn("month", month(col("date")))
raw_data = raw_data.withColumn("year", year(col("date")))

# Window transformations
windowSpec = Window.partitionBy("Device Category").orderBy("timestamp")

raw_data = (
    raw_data.withColumn("row_num", F.row_number().over(windowSpec))
    .withColumn("rank", F.rank().over(windowSpec))
    .withColumn("dense_rank", F.dense_rank().over(windowSpec))
    .withColumn("count", F.count("*").over(windowSpec))
    .withColumn("first", F.first("# of Visitors").over(windowSpec))
    .withColumn("last", F.last("# of Visitors").over(windowSpec))
    .withColumn("min", F.min("# of Visitors").over(windowSpec))
    .withColumn("max", F.max("# of Visitors").over(windowSpec))
    .withColumn("nth", F.nth_value("# of Visitors", 2).over(windowSpec))
    .withColumn("lag", F.lag("# of Visitors", 1).over(windowSpec))
    .withColumn("lead", F.lead("# of Visitors", 1).over(windowSpec))
    .withColumn("percent", F.percent_rank().over(windowSpec))
    .withColumn("ntile", F.ntile(2).over(windowSpec))
    .orderBy("Device Category", "timestamp")
)

# Cast columns to appropriate types
raw_data = raw_data.withColumn("Sessions", raw_data["Sessions"].cast("int"))
raw_data = raw_data.withColumn("# of Visitors", raw_data["# of Visitors"].cast("int"))
raw_data = raw_data.withColumn("Bounce Rate", raw_data["Bounce Rate"].cast("int"))

# COMMAND ----------

# MAGIC %md Sink the processed dataset to DBFS
# MAGIC

# COMMAND ----------

# Write output
if os.path.exists(OUTPUT_FILE_LOCATION):
    # Delete existing file
    dbutils.fs.rm(OUTPUT_FILE_LOCATION, recurse=True)

raw_data.write.format(FILE_TYPE).mode("overwrite").option(
    "header", FIRST_ROW_IS_HEADER
).option("sep", DELIMITER).save(OUTPUT_FILE_LOCATION)
