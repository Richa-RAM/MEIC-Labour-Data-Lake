-- =====================================================================
--  STEP 5 - Hive DDL
--  External tables over every zone of the data lake. They are EXTERNAL,
--  so DROP TABLE only removes the definition - the files in HDFS stay.
--  This script can be re-run safely.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS meic_analytics
COMMENT 'Middle East Infra Contracting LLC - labour data lake';
USE meic_analytics;

-- Tables from the first Zeppelin version that are replaced below
DROP TABLE IF EXISTS unusual_working_hours;

-- ---------------------------------------------------------------------
-- BRONZE (raw): the CSV files exactly as loaded.
-- OpenCSVSerde is needed because project names contain commas inside
-- quotes, e.g. "38-DXB/SP330 SUPER GATE,DUBAI INTERNATIONAL AIRPORT".
-- (This SerDe reads every column as STRING - types are fixed in silver.)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS labour_raw;
CREATE EXTERNAL TABLE labour_raw (
  nos            STRING,
  work_date      STRING,
  project        STRING,
  type_of_work   STRING,
  supervisor     STRING,
  sub_contractor STRING,
  employee       STRING,
  designation    STRING,
  total_hrs      STRING,
  rate           STRING,
  amount         STRING,
  data_source    STRING,
  division       STRING,
  branch         STRING,
  proj_no        STRING,
  proj_type      STRING
)
PARTITIONED BY (source_file STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION '/meic_datalake/bronze/labour'
TBLPROPERTIES ('skip.header.line.count' = '1');
MSCK REPAIR TABLE labour_raw;

-- ---------------------------------------------------------------------
-- SILVER (cleansed): Parquet, partitioned by year and month
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS labour_cleaned;
CREATE EXTERNAL TABLE labour_cleaned (
  record_id               INT,
  source_row_no           INT,
  work_date               DATE,
  project                 STRING,
  project_number          STRING,
  project_type            STRING,
  is_overhead             BOOLEAN,
  type_of_work            STRING,
  supervisor              STRING,
  sub_contractor          STRING,
  employee                STRING,
  designation             STRING,
  total_hours             DOUBLE,
  rate                    DOUBLE,
  amount                  DOUBLE,
  data_source             STRING,
  division                STRING,
  branch                  STRING,
  flag_zero_hours         BOOLEAN,
  flag_zero_rate          BOOLEAN,
  flag_zero_amount        BOOLEAN,
  flag_overtime           BOOLEAN,
  flag_missing_work_type  BOOLEAN,
  flag_possible_duplicate BOOLEAN,
  flag_amount_mismatch    BOOLEAN,
  is_flagged              BOOLEAN,
  flag_reasons            STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/meic_datalake/silver/labour_cleaned';
MSCK REPAIR TABLE labour_cleaned;

-- ---------------------------------------------------------------------
-- REJECTED: rows that failed a hard rule, with the reason
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS labour_rejected;
CREATE EXTERNAL TABLE labour_rejected (
  record_id      INT,
  source_row_no  INT,
  work_date_raw  STRING,
  work_date      DATE,
  project        STRING,
  project_number STRING,
  type_of_work   STRING,
  employee       STRING,
  designation    STRING,
  total_hours    DOUBLE,
  rate           DOUBLE,
  amount         DOUBLE,
  data_source    STRING,
  reject_reason  STRING
)
STORED AS PARQUET
LOCATION '/meic_datalake/rejected/labour_invalid';

-- ---------------------------------------------------------------------
-- GOLD (curated): business-ready tables
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS project_costs;
CREATE EXTERNAL TABLE project_costs (
  project_number      STRING,
  project             STRING,
  project_type        STRING,
  is_overhead         BOOLEAN,
  total_labour_cost   DOUBLE,
  total_working_hours DOUBLE,
  total_records       BIGINT,
  distinct_employees  BIGINT,
  first_work_date     DATE,
  last_work_date      DATE,
  cost_per_hour       DOUBLE
)
STORED AS PARQUET
LOCATION '/meic_datalake/gold/project_costs';

DROP TABLE IF EXISTS employee_work_hours;
CREATE EXTERNAL TABLE employee_work_hours (
  employee            STRING,
  type_of_work        STRING,
  total_working_hours DOUBLE,
  total_labour_cost   DOUBLE,
  total_records       BIGINT,
  days_worked         BIGINT
)
STORED AS PARQUET
LOCATION '/meic_datalake/gold/employee_work_hours';

DROP TABLE IF EXISTS flagged_records;
CREATE EXTERNAL TABLE flagged_records (
  record_id               INT,
  work_date               DATE,
  data_source             STRING,
  employee                STRING,
  designation             STRING,
  project                 STRING,
  type_of_work            STRING,
  total_hours             DOUBLE,
  rate                    DOUBLE,
  amount                  DOUBLE,
  flag_overtime           BOOLEAN,
  flag_zero_hours         BOOLEAN,
  flag_zero_rate          BOOLEAN,
  flag_zero_amount        BOOLEAN,
  flag_missing_work_type  BOOLEAN,
  flag_possible_duplicate BOOLEAN,
  flag_amount_mismatch    BOOLEAN,
  flag_reasons            STRING
)
STORED AS PARQUET
LOCATION '/meic_datalake/gold/flagged_records';

DROP TABLE IF EXISTS yearly_trends;
CREATE EXTERNAL TABLE yearly_trends (
  year                INT,
  total_labour_cost   DOUBLE,
  total_working_hours DOUBLE,
  total_records       BIGINT,
  distinct_employees  BIGINT,
  distinct_projects   BIGINT,
  months_covered      BIGINT,
  overhead_cost       DOUBLE,
  cost_per_hour       DOUBLE
)
STORED AS PARQUET
LOCATION '/meic_datalake/gold/yearly_trends';

DROP TABLE IF EXISTS monthly_trends;
CREATE EXTERNAL TABLE monthly_trends (
  year                INT,
  month               INT,
  total_labour_cost   DOUBLE,
  total_working_hours DOUBLE,
  total_records       BIGINT
)
STORED AS PARQUET
LOCATION '/meic_datalake/gold/monthly_trends';

DROP TABLE IF EXISTS data_quality_summary;
CREATE EXTERNAL TABLE data_quality_summary (
  check_name   STRING,
  record_count BIGINT
)
STORED AS PARQUET
LOCATION '/meic_datalake/gold/data_quality_summary';

-- ---------------------------------------------------------------------
-- Evidence for the report
-- ---------------------------------------------------------------------
SHOW TABLES IN meic_analytics;
DESCRIBE labour_cleaned;
DESCRIBE FORMATTED project_costs;
SHOW PARTITIONS labour_raw;
SHOW PARTITIONS labour_cleaned;
