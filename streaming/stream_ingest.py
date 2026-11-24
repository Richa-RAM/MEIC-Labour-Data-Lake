#!/usr/bin/env python3
"""
STEP 7 (part 2) - Spark Structured Streaming consumer

Watches the HDFS landing folder for new timesheet files, applies the same
basic cleaning as the batch job, appends each micro-batch to
bronze/stream/timesheet (Parquet) and prints a summary of each batch.

Usage:
  spark-submit --master local[*] stream_ingest.py hdfs:///datalake [run_seconds]
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BASE = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///datalake"
RUN_SECONDS = int(sys.argv[2]) if len(sys.argv) > 2 else 120

spark = (SparkSession.builder.appName("MEIC-03-timesheet-stream")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.ansi.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

COLS = ["nos", "date", "project", "type_of_work", "supervisor", "sub_contractor", "employee",
        "designation", "total_hours", "rate", "amount", "data_source", "division", "branch",
        "project_number", "project_type"]
schema = StructType([StructField(c, StringType()) for c in COLS])  # streaming needs a fixed schema

stream = (spark.readStream
          .schema(schema)
          .option("header", "true").option("quote", '"').option("escape", '"')
          .option("maxFilesPerTrigger", 1)                 # one file = one micro-batch
          .csv(BASE + "/landing/timesheet_stream"))

clean = (stream
         .withColumn("work_date", F.to_date("date", "dd-MMM-yy"))
         .withColumn("total_hours", F.col("total_hours").cast("double"))
         .withColumn("rate", F.col("rate").cast("double"))
         .withColumn("amount", F.col("amount").cast("double"))
         .withColumn("data_source", F.when(F.col("data_source") == "Timehseet", "Timesheet")
                                     .otherwise(F.col("data_source")))
         .withColumn("is_valid", F.col("work_date").isNotNull() & F.col("project").isNotNull()
                     & (F.col("amount") >= 0) & (F.col("total_hours").between(0, 24)))
         .withColumn("ingested_at", F.current_timestamp())
         .drop("date"))


def handle_batch(batch_df, batch_id):
    batch_df.persist()
    n = batch_df.count()
    if n == 0:
        batch_df.unpersist()
        return
    batch_df.write.mode("append").parquet(BASE + "/bronze/stream/timesheet")
    s = batch_df.agg(F.min("work_date").alias("from_date"), F.max("work_date").alias("to_date"),
                     F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("invalid"),
                     F.round(F.sum("amount"), 2).alias("labour_cost")).first()
    print("[batch {}] {} rows | {} .. {} | invalid={} | labour cost={}".format(
        batch_id, n, s["from_date"], s["to_date"], s["invalid"], s["labour_cost"]), flush=True)
    batch_df.unpersist()


query = (clean.writeStream
         .foreachBatch(handle_batch)
         .option("checkpointLocation", BASE + "/landing/_checkpoints/timesheet_stream")
         .trigger(processingTime="5 seconds")
         .start())

print("Streaming job started - watching {}/landing/timesheet_stream for {} s".format(BASE, RUN_SECONDS))
query.awaitTermination(RUN_SECONDS)
query.stop()
try:
    total = spark.read.parquet(BASE + "/bronze/stream/timesheet").count()
except Exception:
    total = 0
print("Streaming job stopped. Rows now in bronze/stream/timesheet: {}".format(total))
spark.stop()
