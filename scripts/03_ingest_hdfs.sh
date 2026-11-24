#!/usr/bin/env bash
# =====================================================================
#  STEP 3 - Ingestion
#  Creates the data lake zones in HDFS and loads the three source files
#  into the bronze (raw) zone, one folder per source system:
#
#    /datalake/bronze/labour/source_file=DA/DA.csv
#    /datalake/bronze/labour/source_file=Timesheet/Timesheet.csv
#    /datalake/bronze/labour/source_file=Veh_Activity/Veh_Activity.csv
#
#  The "source_file=" folder names let Spark and Hive treat each source
#  as a partition.
# =====================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/config.env"
export PATH="$HADOOP_HOME/bin:$PATH"

B="$HDFS_BASE"

echo "### Creating zones"
hdfs dfs -mkdir -p "$B/bronze" "$B/silver" "$B/gold" "$B/rejected" "$B/landing"

echo "### Loading source files into bronze"
hdfs dfs -rm -r -f -skipTrash "$B/bronze/labour" >/dev/null 2>&1 || true
for f in "$STAGING_DIR"/*.csv; do
  name="$(basename "$f" .csv)"
  hdfs dfs -mkdir -p "$B/bronze/labour/source_file=$name"
  hdfs dfs -put -f "$f" "$B/bronze/labour/source_file=$name/"
  echo "  loaded $f"
done

echo
echo "### hdfs dfs -ls $B"
hdfs dfs -ls "$B"
echo
echo "### hdfs dfs -ls -R $B/bronze/labour"
hdfs dfs -ls -R "$B/bronze/labour"
echo
echo "### Line counts in HDFS (each includes 1 header line)"
for d in $(hdfs dfs -ls "$B/bronze/labour" | awk '{print $NF}' | grep source_file=); do
  printf "  %-55s %s\n" "$d" "$(hdfs dfs -cat "$d"/*.csv | wc -l)"
done
