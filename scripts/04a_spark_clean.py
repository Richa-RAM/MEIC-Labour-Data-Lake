#!/usr/bin/env python3
"""
STEP 4a - Spark cleaning job: bronze (raw) -> silver (cleansed) + rejected

Run with:  spark-submit --master local[*] 04a_spark_clean.py hdfs:///datalake

What it does
  1. Reads all raw CSVs from bronze/labour (one folder per source system)
  2. Standardises column names and types
       - snake_case names, "Proj.No." -> project_number (kept as text: '60', 'OH-01')
       - Date '25-Jan-17' -> DATE, numbers -> DOUBLE
       - trims text, empty text -> NULL
       - fixes the "Timehseet" source label -> "Timesheet"
       - missing / '-Select-' Type of Work -> 'Unspecified'
       - adds record_id (the source 'Nos' column is NOT unique) and is_overhead
  3. REJECTS rows that cannot be used (written to /rejected with a reason)
       - missing date, project or employee
       - negative hours, rate or amount
       - more than 24 hours in one day (impossible)
  4. FLAGS rows that are usable but suspicious (kept in silver with flag columns)
       - zero hours / zero rate / zero amount
       - overtime: more than 12 hours in one day
       - missing type of work
       - possible duplicate: identical to another row in every field except Nos
       - amount differs from hours x rate by more than 20% (and more than 1 AED)
  5. Writes silver as Parquet partitioned by year and month,
     plus a data-quality summary table in gold.
"""
import sys
from collections import OrderedDict   # Python 3.4: plain dicts are unordered
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///datalake"

spark = (SparkSession.builder.appName("MEIC-01-clean-labour")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")   # Spark 3: no parser-upgrade errors
         .config("spark.sql.ansi.enabled", "false")                  # Spark 4: bad values -> NULL, not crash
         .config("spark.sql.sources.partitionOverwriteMode", "static")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Read bronze (all columns as text first; types are set explicitly below)
# ---------------------------------------------------------------------------
raw = (spark.read
       .option("header", "true")
       .option("quote", '"').option("escape", '"')      # project names contain commas
       .csv(BASE + "/bronze/labour"))                    # adds source_file from folder names

RENAME = {
    "Nos": "source_row_no", "Date": "work_date_raw", "Project": "project",
    "Type of Work": "type_of_work", "Supervisor": "supervisor",
    "Sub Contractor": "sub_contractor", "Employee": "employee",
    "Designation": "designation", "Total Hrs": "total_hours", "Rate": "rate",
    "Amount": "amount", "Data Source": "data_source", "Division": "division",
    "Branch": "branch", "Proj.No.": "project_number", "Proj.Type": "project_type",
    "source_file": "source_file",
}
# Rename by position so names with dots/spaces/BOM never cause problems
clean_names = [RENAME.get(c.replace("﻿", "").strip(), c) for c in raw.columns]
df = raw.toDF(*clean_names)

raw_count = df.count()
print("\n================ BEFORE CLEANING ================")
print("Raw records read from bronze : {:,}".format(raw_count))
df.groupBy("source_file").count().orderBy("source_file").show(truncate=False)

# ---------------------------------------------------------------------------
# 2. Standardise
# ---------------------------------------------------------------------------
TEXT_COLS = ["project", "type_of_work", "supervisor", "sub_contractor", "employee",
             "designation", "data_source", "division", "branch", "project_number",
             "project_type"]
for c in TEXT_COLS:
    df = df.withColumn(c, F.trim(F.col(c)))
    df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

df = (df
      # fix the misspelt source label
      .withColumn("data_source", F.when(F.col("data_source") == "Timehseet", "Timesheet")
                                  .otherwise(F.col("data_source")))
      # types
      .withColumn("source_row_no", F.col("source_row_no").cast("int"))
      .withColumn("work_date", F.coalesce(F.to_date(F.trim("work_date_raw"), "dd-MMM-yy"),
                                          F.to_date(F.trim("work_date_raw"), "yyyy-MM-dd")))
      .withColumn("total_hours", F.col("total_hours").cast("double"))
      .withColumn("rate", F.col("rate").cast("double"))
      .withColumn("amount", F.col("amount").cast("double"))
      .withColumn("project_number", F.upper(F.col("project_number")))
      .withColumn("is_overhead", F.coalesce(F.col("project_number").startswith("OH-")
                                            | (F.col("project_type") == "Overhead"), F.lit(False)))
      # missing work type: remember it, then label it
      .withColumn("flag_missing_work_type",
                  F.col("type_of_work").isNull() | (F.col("type_of_work") == "-Select-"))
      .withColumn("type_of_work", F.when(F.col("flag_missing_work_type"), "Unspecified")
                                   .otherwise(F.col("type_of_work")))
      .withColumn("year", F.year("work_date"))
      .withColumn("month", F.month("work_date")))

# Surrogate key: 'Nos' repeats within DA, so build a unique, stable record_id
key_window = Window.orderBy("source_file", "source_row_no", "work_date", "employee", "project")
df = df.withColumn("record_id", F.row_number().over(key_window))

# ---------------------------------------------------------------------------
# 3. Reject rules (row cannot be used)
# ---------------------------------------------------------------------------
reject_rules = [
    (F.col("work_date").isNull(), "missing or invalid date"),
    (F.col("project").isNull(), "missing project"),
    (F.col("employee").isNull(), "missing employee"),
    (F.col("total_hours").isNull() | F.col("rate").isNull() | F.col("amount").isNull(),
     "missing hours/rate/amount"),
    (F.col("total_hours") < 0, "negative hours"),
    (F.col("rate") < 0, "negative rate"),
    (F.col("amount") < 0, "negative amount"),
    (F.col("total_hours") > 24, "more than 24 hours in one day"),
]
df = df.withColumn("reject_reason",
                   F.concat_ws("; ", *[F.when(cond, F.lit(txt)) for cond, txt in reject_rules]))
df = df.withColumn("is_rejected", F.col("reject_reason") != "")

# ---------------------------------------------------------------------------
# 4. Flag rules (row is kept but marked for review)
# ---------------------------------------------------------------------------
business_cols = ["work_date", "project", "type_of_work", "supervisor", "sub_contractor",
                 "employee", "designation", "total_hours", "rate", "amount",
                 "data_source", "division", "branch", "project_number", "project_type"]
dup_window = Window.partitionBy(*business_cols)
expected = F.col("total_hours") * F.col("rate")

df = (df
      .withColumn("flag_zero_hours", F.col("total_hours") == 0)
      .withColumn("flag_zero_rate", F.col("rate") == 0)
      .withColumn("flag_zero_amount", F.col("amount") == 0)
      .withColumn("flag_overtime", F.col("total_hours") > 12)
      .withColumn("flag_possible_duplicate", F.count(F.lit(1)).over(dup_window) > 1)
      .withColumn("flag_amount_mismatch",
                  (F.abs(F.col("amount") - expected) > F.greatest(F.lit(1.0), expected * 0.2))))

FLAGS = OrderedDict([
    ("flag_zero_hours", "zero hours"),
    ("flag_zero_rate", "zero rate"),
    ("flag_zero_amount", "zero amount"),
    ("flag_overtime", "over 12 hours"),
    ("flag_missing_work_type", "missing type of work"),
    ("flag_possible_duplicate", "possible duplicate"),
    ("flag_amount_mismatch", "amount != hours x rate"),
])
for c in FLAGS:
    df = df.withColumn(c, F.coalesce(F.col(c), F.lit(False)))
df = (df.withColumn("flag_reasons",
                    F.concat_ws("; ", *[F.when(F.col(c), F.lit(txt)) for c, txt in FLAGS.items()]))
        .withColumn("is_flagged", F.col("flag_reasons") != ""))

df.cache()

# ---------------------------------------------------------------------------
# 5. Split and write
# ---------------------------------------------------------------------------
SILVER_COLS = (["record_id", "source_row_no", "work_date", "project", "project_number",
                "project_type", "is_overhead", "type_of_work", "supervisor", "sub_contractor",
                "employee", "designation", "total_hours", "rate", "amount", "data_source",
                "division", "branch"]
               + list(FLAGS) + ["is_flagged", "flag_reasons", "year", "month"])
REJECT_COLS = (["record_id", "source_row_no", "work_date_raw", "work_date", "project",
                "project_number", "type_of_work", "employee", "designation", "total_hours",
                "rate", "amount", "data_source", "reject_reason"])

silver = df.filter(~F.col("is_rejected")).select(*SILVER_COLS)
rejected = df.filter(F.col("is_rejected")).select(*REJECT_COLS)

(silver.write.mode("overwrite").partitionBy("year", "month")
       .parquet(BASE + "/silver/labour_cleaned"))
rejected.coalesce(1).write.mode("overwrite").parquet(BASE + "/rejected/labour_invalid")

# ---------------------------------------------------------------------------
# 6. Data-quality summary (printed + saved to gold for Hive)
# ---------------------------------------------------------------------------
silver_n = silver.count()
rejected_n = rejected.count()
flagged_n = silver.filter("is_flagged").count()

summary = [("raw records (bronze)", raw_count),
           ("clean records (silver)", silver_n),
           ("rejected records", rejected_n),
           ("flagged records kept in silver", flagged_n),
           ("source label fixed (Timehseet -> Timesheet)",
            raw.filter(F.trim(F.col(raw.columns[11])) == "Timehseet").count())]
for cond, txt in reject_rules:
    summary.append(("rejected: " + txt, df.filter(cond).count()))
for c, txt in FLAGS.items():
    summary.append(("flagged: " + txt, silver.filter(F.col(c)).count()))

dq = spark.createDataFrame(summary, "check_name string, record_count long")
dq.coalesce(1).write.mode("overwrite").parquet(BASE + "/gold/data_quality_summary")

print("\n================ AFTER CLEANING ================")
print("Silver (clean) records : {:,}".format(silver_n))
print("Rejected records       : {:,}".format(rejected_n))
print("Total accounted for    : {:,}  (raw = {:,})".format(silver_n + rejected_n, raw_count))
assert silver_n + rejected_n == raw_count, "Row count does not reconcile!"
print("\nData-quality summary")
dq.show(50, truncate=False)

print("Rejected records")
rejected.select("record_id", "work_date", "employee", "project", "total_hours",
                "rate", "amount", "reject_reason").orderBy("record_id").show(20, truncate=False)

print("Sample of flagged records")
silver.filter("is_flagged").select("record_id", "work_date", "employee", "type_of_work",
                                   "total_hours", "rate", "amount", "flag_reasons") \
      .show(15, truncate=False)

print("Silver partitions written (year/month):")
silver.groupBy("year").agg(F.countDistinct("month").alias("months"),
                           F.count(F.lit(1)).alias("records")).orderBy("year").show()
spark.stop()
