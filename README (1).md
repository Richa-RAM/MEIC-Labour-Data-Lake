# MEIC Labour Data Lake

An automated **batch-processing data lake** for labour records from Middle East Infra Contracting LLC (MEIC). The project uses **Hadoop HDFS** for storage, **Apache Spark (PySpark)** for data preparation and analytics, and **Apache Hive** for SQL querying. The pipeline is designed to run in an Ubuntu VirtualBox virtual machine.

## Project overview

The input contains labour records from **2017 to March 2026**. The pipeline separates the records into three source categories, stores raw data in the Bronze layer, cleans and validates records in the Silver layer, retains rejected records separately, and produces six business-oriented Gold datasets. Hive external tables make the data available for analysis.

This repository implements a **six-stage automated batch-processing pipeline**.

## Architecture

```text
Local source: labour_raw.csv (or supported Data.xlsx)
                 |
                 v
Stage 2: Prepare and split source records
        DA.csv | Timesheet.csv | Veh_Activity.csv
                 |
                 v
Stage 3: HDFS Bronze
        /meic_datalake/bronze/labour/
                 |
                 v
Stage 4a: Spark validation and cleaning
          |                      |
          v                      v
  Silver (valid)          Rejected (invalid)
  /meic_datalake/         /meic_datalake/
  silver/labour_cleaned   rejected/labour_invalid
          |
          v
Stage 4b: Spark Gold datasets
        /meic_datalake/gold/
                 |
                 v
Stage 5: Hive external tables (meic_analytics)
                 |
                 v
Stage 6: Hive business queries
```

Stage 1 checks the environment before processing. Stage 4 consists of two Spark jobs: cleaning and Gold-layer curation.

## Technologies

- Ubuntu Linux in Oracle VirtualBox
- Apache Hadoop (HDFS and YARN)
- Apache Spark with PySpark
- Apache Hive and HiveQL
- Apache Zeppelin for optional exploration and visualisation
- Python 3 and Bash

## Repository structure

```text
MEIC-Labour-Data-Lake/
├── README.md
├── .gitignore
├── config.env                  # VM and pipeline configuration
├── run_pipeline.sh             # Runs stages 1–6
├── data/                       # Optional source dataset (if permitted to share)
│   └── labour_raw.csv
└── scripts/
    ├── 00_start_services.sh   # Start Hadoop/Hive/Zeppelin services
    ├── 01_check_env.sh        # Check environment and software
    ├── 02_prepare_data.py     # Split source data into three CSV files
    ├── 03_ingest_hdfs.sh      # Load Bronze data into HDFS
    ├── 04a_spark_clean.py     # Validate, clean, flag, and reject records
    ├── 04b_spark_gold.py      # Create Gold analytics datasets
    ├── 05_hive_ddl.hql        # Register Hive external tables
    └── 06_business_queries.hql # Run analytical queries
```

The `data/` directory is only present after the source dataset has been added. Do not commit confidential employee records to a public repository without permission.

## Configuration

Edit `config.env` for the paths and services on your VM. The supplied configuration uses:

```bash
HADOOP_HOME=/usr/share/hadoop
SPARK_HOME=/usr/share/spark
HIVE_HOME=/usr/share/hive
SOURCE_FILE=/home/bigdata/labour_raw.csv
STAGING_DIR=/home/bigdata/meic_staging
HDFS_BASE=/meic_datalake
SPARK_MASTER='local[*]'
HIVE_MODE=hive
```

**Important:** `SOURCE_FILE` is an absolute path in the VM. If you obtain the dataset from `data/labour_raw.csv` in this repository, either copy it to `/home/bigdata/labour_raw.csv` or change `SOURCE_FILE` in `config.env` to its actual location. Check configuration files for secrets before publishing.

## Running the pipeline

Run the following commands in the Ubuntu VM, from the repository directory:

```bash
cd ~/meic_github_submission

# Review configuration first
nano config.env

# Start required services (if they are not already running)
bash scripts/00_start_services.sh

# Run the complete batch pipeline (stages 1–6)
bash run_pipeline.sh
```

You can also run a selected stage range:

```bash
bash run_pipeline.sh 2 5    # Prepare, ingest, process, and register tables
bash run_pipeline.sh 6 6    # Run business queries only
```

Stages assume that the required input data and preceding HDFS outputs already exist. Running stages 2–5 recreates the processed datasets and Hive table definitions. The main script saves execution logs under `logs/` (ignored by Git).

## Processing stages

| Stage | Script | Purpose |
|---|---|---|
| 1 | `scripts/01_check_env.sh` | Check Hadoop, Spark, Hive and the runtime environment |
| 2 | `scripts/02_prepare_data.py` | Separate source records into DA, Timesheet and Vehicle Activity files |
| 3 | `scripts/03_ingest_hdfs.sh` | Create HDFS data lake directories and ingest Bronze CSV files |
| 4a | `scripts/04a_spark_clean.py` | Clean data, validate records, flag issues, write Silver and Rejected data |
| 4b | `scripts/04b_spark_gold.py` | Generate six Gold datasets for reporting and analysis |
| 5 | `scripts/05_hive_ddl.hql` | Create external Hive tables in `meic_analytics` |
| 6 | `scripts/06_business_queries.hql` | Run HiveQL business-analysis queries |

## Data layers and outputs

**Bronze:** Original source records, separated by source category and stored as CSV files.

**Silver:** Cleaned and standardised records stored as Parquet, including issue flags for records that can still be analysed.

**Rejected:** Invalid records stored separately with rejection reasons.

**Gold:** Six analytical datasets:

1. `project_costs` — Labour cost and working hours by project.
2. `employee_work_hours` — Employee working hours and labour cost.
3. `flagged_records` — Records identified for further review.
4. `yearly_trends` — Annual labour trends.
5. `monthly_trends` — Monthly labour trends.
6. `data_quality_summary` — Summary of data-quality findings.

Hive registers **nine external tables**: one raw table, one cleaned table, one rejected table and six Gold tables. The tables reference data in HDFS rather than copying it into Hive-managed storage.

## Data quality rules

The cleaning job rejects invalid records, including records with missing required fields, negative numerical values or working hours exceeding the daily limit. It also flags potentially unusual records (such as long shifts, missing work types, possible duplicates and discrepancies between hours, rate and amount) while retaining eligible records for analysis.

The scripts standardise field names, parse dates and numerical values, and prepare data for partitioned storage and analytical queries.

## Dataset and verification

The source dataset used during development contained **219,862 records**:

| Source category | Records |
|---|---:|
| DA | 152,505 |
| Timesheet | 54,612 |
| Vehicle Activity | 12,745 |
| **Total** | **219,862** |

These are source-split counts from the development run. Final Silver, Rejected and flagged counts should be taken from the logs of the submitted version of the pipeline, rather than assumed from the README.

To inspect the results after a successful run, use Hive:

```bash
hive -e 'USE meic_analytics; SHOW TABLES;'
hive -e 'USE meic_analytics; SELECT * FROM yearly_trends LIMIT 10;'
```

You can also inspect HDFS output directories with `hdfs dfs -ls /meic_datalake/gold/` and explore the registered Hive tables using Zeppelin.

## Notes

- Spark is configured to use `local[*]` by default on the VM.
- Hive tables are **EXTERNAL**, so dropping their definitions does not delete the underlying HDFS data.
- Execution logs are generated locally in `logs/` and are excluded from Git by `.gitignore`.
- Paths and software versions may need adjustment on a different VM.
