#!/usr/bin/env bash
# =====================================================================
#  Middle East Infra Contracting LLC - Labour Data Lake
#  Runs the whole pipeline end to end and saves every step's output
#  in logs/ (use these for your report screenshots).
#
#  Usage:
#    ./run_pipeline.sh            run steps 1-6
#    ./run_pipeline.sh 4          run steps 4-6 (start from step 4)
#    ./run_pipeline.sh 5 5        run step 5 only
#
#  Steps:
#    1  Check environment (jps, versions, web UIs)
#    2  Prepare data: split the source file into DA / Timesheet / Veh.Activity
#    3  Ingest: create HDFS zones and load the files into bronze
#    4  Spark: clean bronze -> silver (+ rejected), then build gold tables
#    5  Hive DDL: external tables over bronze, silver, rejected and gold
#    6  Business queries in Hive (with run times)
#  Step 7 (streaming simulation) is separate: streaming/run_streaming_demo.sh
# =====================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/config.env"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin:$PATH"

START="${1:-1}"
END="${2:-6}"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
HDFS_URI="hdfs://${HDFS_BASE}"

run_hive() {   # run_hive <file.hql>
  if [ "$HIVE_MODE" = "beeline" ]; then
    beeline -u "$HIVE_JDBC_URL" -n "$HIVE_USER" --showHeader=true --outputformat=table2 -f "$1"
  else
    hive -f "$1"
  fi
}

step() {       # step <number> <title> <command...>
  local n="$1" title="$2"; shift 2
  if [ "$n" -lt "$START" ] || [ "$n" -gt "$END" ]; then return 0; fi
  local log="$LOG_DIR/${RUN_ID}_step${n}.log"
  echo
  echo "=================================================================="
  echo " STEP $n: $title"
  echo " log: $log"
  echo "=================================================================="
  local t0=$SECONDS
  "$@" 2>&1 | tee "$log"
  echo "--- step $n finished in $((SECONDS - t0)) s ---" | tee -a "$log"
}

step 1 "Check environment" \
  bash "$ROOT/scripts/01_check_env.sh"

step 2 "Prepare data (split into source files)" \
  python3 "$ROOT/scripts/02_prepare_data.py" "$SOURCE_FILE" "$STAGING_DIR"

step 3 "Ingest into HDFS bronze zone" \
  bash "$ROOT/scripts/03_ingest_hdfs.sh"

step 4 "Spark: clean (bronze -> silver) and curate (silver -> gold)" \
  bash -c "spark-submit --master '$SPARK_MASTER' --name MEIC-clean '$ROOT/scripts/04a_spark_clean.py' '$HDFS_URI' \
        && spark-submit --master '$SPARK_MASTER' --name MEIC-gold  '$ROOT/scripts/04b_spark_gold.py'  '$HDFS_URI'"

step 5 "Hive DDL (external tables)" \
  run_hive "$ROOT/scripts/05_hive_ddl.hql"

step 6 "Business queries" \
  run_hive "$ROOT/scripts/06_business_queries.hql"

echo
echo "Pipeline finished. Logs are in $LOG_DIR (prefix $RUN_ID)."
