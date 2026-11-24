#!/usr/bin/env python3
"""
STEP 4b - Spark curation job: silver (cleansed) -> gold (curated, business-ready)

Run with:  spark-submit --master local[*] 04b_spark_gold.py hdfs:///datalake

Gold tables (Parquet), one per business question:
  project_costs         labour cost and hours per project (overhead marked separately)
  employee_work_hours   hours and cost per employee and activity (type of work)
  flagged_records       every record that failed a data-quality check, with the reasons
  yearly_trends         labour cost per year (with months covered, so the part-year 2026 is visible)
  monthly_trends        labour cost per month, for trend charts
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///datalake"

spark = SparkSession.builder.appName("MEIC-02-gold-tables").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(BASE + "/silver/labour_cleaned")   # year/month come back from the folders
silver.cache()
print("Silver records read: {:,}".format(silver.count()))


def write_gold(df, name, sort_col, n=10):
    df.coalesce(1).write.mode("overwrite").parquet(BASE + "/gold/" + name)
    print("\n=== gold/{}  ({:,} rows) ===".format(name, df.count()))
    df.orderBy(F.col(sort_col).desc()).show(n, truncate=False)


# 1. Cost per project ---------------------------------------------------------
project_costs = (silver
    .groupBy("project_number", "project", "project_type", "is_overhead")
    .agg(F.round(F.sum("amount"), 2).alias("total_labour_cost"),
         F.round(F.sum("total_hours"), 2).alias("total_working_hours"),
         F.count(F.lit(1)).alias("total_records"),
         F.countDistinct("employee").alias("distinct_employees"),
         F.min("work_date").alias("first_work_date"),
         F.max("work_date").alias("last_work_date"))
    .withColumn("cost_per_hour",
                F.round(F.col("total_labour_cost") / F.col("total_working_hours"), 2)))
write_gold(project_costs, "project_costs", "total_labour_cost")

# 2. Hours per employee and activity -----------------------------------------
employee_work_hours = (silver
    .groupBy("employee", "type_of_work")
    .agg(F.round(F.sum("total_hours"), 2).alias("total_working_hours"),
         F.round(F.sum("amount"), 2).alias("total_labour_cost"),
         F.count(F.lit(1)).alias("total_records"),
         F.countDistinct("work_date").alias("days_worked")))
write_gold(employee_work_hours, "employee_work_hours", "total_working_hours")

# 3. Flagged records -----------------------------------------------------------
flagged_records = (silver.filter("is_flagged")
    .select("record_id", "work_date", "data_source", "employee", "designation", "project",
            "type_of_work", "total_hours", "rate", "amount", "flag_overtime",
            "flag_zero_hours", "flag_zero_rate", "flag_zero_amount", "flag_missing_work_type",
            "flag_possible_duplicate", "flag_amount_mismatch", "flag_reasons"))
write_gold(flagged_records, "flagged_records", "total_hours")

# 4. Yearly trend -------------------------------------------------------------
yearly_trends = (silver
    .groupBy("year")
    .agg(F.round(F.sum("amount"), 2).alias("total_labour_cost"),
         F.round(F.sum("total_hours"), 2).alias("total_working_hours"),
         F.count(F.lit(1)).alias("total_records"),
         F.countDistinct("employee").alias("distinct_employees"),
         F.countDistinct("project").alias("distinct_projects"),
         F.countDistinct("month").alias("months_covered"),
         F.round(F.sum(F.when(F.col("is_overhead"), F.col("amount")).otherwise(0)), 2)
          .alias("overhead_cost"))
    .withColumn("cost_per_hour",
                F.round(F.col("total_labour_cost") / F.col("total_working_hours"), 2)))
yearly_trends.coalesce(1).write.mode("overwrite").parquet(BASE + "/gold/yearly_trends")
print("\n=== gold/yearly_trends ===")
yearly_trends.orderBy("year").show(20, truncate=False)

# 5. Monthly trend ------------------------------------------------------------
monthly_trends = (silver
    .groupBy("year", "month")
    .agg(F.round(F.sum("amount"), 2).alias("total_labour_cost"),
         F.round(F.sum("total_hours"), 2).alias("total_working_hours"),
         F.count(F.lit(1)).alias("total_records")))
monthly_trends.coalesce(1).write.mode("overwrite").parquet(BASE + "/gold/monthly_trends")
print("\n=== gold/monthly_trends ({} months) ===".format(monthly_trends.count()))
monthly_trends.orderBy("year", "month").show(12, truncate=False)

spark.stop()
