# Middle East Infra Contracting LLC — Labour Data Lake

Hadoop data lake that stores the company's labour records (DA, Timesheet and
Vehicle Activity, 2017 – March 2026). Spark cleans the data and Hive makes it
queryable with SQL.

```
labour_raw.csv / Data.xlsx
      │  step 2  split by source system
      ▼
HDFS /datalake/bronze/labour/source_file=DA|Timesheet|Veh_Activity   (raw CSV)
      │  step 4a Spark clean
      ├──────────────► /datalake/rejected/labour_invalid            (Parquet + reason)
      ▼
HDFS /datalake/silver/labour_cleaned/year=YYYY/month=M              (Parquet, flagged)
      │  step 4b Spark curate
      ▼
HDFS /datalake/gold/{project_costs, employee_work_hours, flagged_records,
                     yearly_trends, monthly_trends, data_quality_summary}
      │  step 5  Hive external tables  →  step 6  business queries (HiveQL)
```

## How to run (on the VM, as user `bigdata`)

```bash
unzip meic_datalake.zip && cd meic_datalake
nano config.env                       # check HADOOP_HOME, SPARK_HOME, HIVE_HOME, SOURCE_FILE
bash scripts/00_start_services.sh     # start HDFS, YARN, Hive metastore, HiveServer2, Zeppelin
./run_pipeline.sh                     # steps 1-6
bash streaming/run_streaming_demo.sh  # step 7 (optional)
```

## Asking questions from the terminal (after the pipeline has run)

```bash
./ask.sh                                   # menu: tables, business questions, previews
./ask.sh "SELECT * FROM yearly_trends;"    # one query
./ask.sh shell                             # Hive SQL prompt (type !quit to leave)
```

Re-run part of the pipeline: `./run_pipeline.sh 4` (steps 4–6) or `./run_pipeline.sh 6 6` (queries only).
Every step's output is saved in `logs/` — those are your screenshots.

| Step | File | Evidence it produces |
|---|---|---|
| 1 | `scripts/00_start_services.sh`, `scripts/01_check_env.sh` | `jps`, versions, web-UI addresses |
| 2 | `scripts/02_prepare_data.py` | row count per source file |
| 3 | `scripts/03_ingest_hdfs.sh` | `hdfs dfs -ls` listing and line counts |
| 4 | `scripts/04a_spark_clean.py`, `scripts/04b_spark_gold.py` | before/after counts, rejected and flagged records, gold tables |
| 5 | `scripts/05_hive_ddl.hql` | `SHOW TABLES`, `DESCRIBE`, `SHOW PARTITIONS` |
| 6 | `scripts/06_business_queries.hql` | query results + run times (`N rows selected (x seconds)`) |
| 7 | `streaming/run_streaming_demo.sh` | producer sending batches, consumer printing each batch |

## Expected results (from `labour_raw.csv`)

| Check | Records |
|---|---|
| Raw (bronze) | 219,862 — DA 152,505 · Timesheet 54,612 · Veh_Activity 12,745 |
| Clean (silver) | 219,760 |
| Rejected | 102 — 94 with more than 24 hours in a day, 4 missing project, 4 negative amount |
| Flagged, kept in silver | 68,144 (one record can have several flags) |
| – over 12 hours | 25,995 |
| – missing type of work (blank or "-Select-") | 37,421 |
| – amount ≠ hours × rate (by more than 20%) | 9,123 |
| – possible duplicate | 915 |
| – zero amount / zero rate / zero hours | 331 / 292 / 46 |
| "Timehseet" label fixed | 54,612 |

Overhead (OH- projects) accounts for about 47% of the clean labour cost (AED 9.0M of 19.0M).

## Cleaning rules

**Rejected** (moved to `/datalake/rejected` with a `reject_reason`): missing date, project
or employee; negative hours, rate or amount; more than 24 hours in one day.

**Flagged** (kept in silver, with `flag_*` columns and `flag_reasons`): zero hours, rate or amount;
more than 12 hours; missing type of work; possible duplicate (same in every field except `Nos`);
amount differs from hours × rate by more than 20%.

**Standardised**: snake_case column names; `Proj.No.` → `project_number` (text, because of
values like `OH-01`); dates → `DATE`; numbers → `DOUBLE`; trimmed text; `Timehseet` → `Timesheet`;
`is_overhead` added; new unique `record_id` added because the source `Nos` repeats within DA.

## Notes

* Spark runs with `local[*]` by default. Set `SPARK_MASTER="yarn"` in `config.env` to run on YARN.
* The raw Hive table uses `OpenCSVSerde` because some project names contain commas.
* All Hive tables are `EXTERNAL`, so `05_hive_ddl.hql` can be re-run without losing data.
* Step 7 uses Spark Structured Streaming's file source.
