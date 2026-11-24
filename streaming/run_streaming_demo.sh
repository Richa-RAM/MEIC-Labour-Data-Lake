#!/usr/bin/env bash
# =====================================================================
#  STEP 7 - Streaming simulation
#  Starts the Spark Structured Streaming job, then replays timesheet rows
#  into HDFS as small files, so you can watch the messages flow through.
#  Output is saved to logs/streaming_*.log.
#
#  Usage: bash streaming/run_streaming_demo.sh [batches] [rows_per_batch] [seconds_between]
# =====================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/config.env"
export PATH="$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"

BATCHES="${1:-12}"
ROWS="${2:-500}"
PAUSE="${3:-5}"
RUN_SECONDS=$(( BATCHES * ${PAUSE%.*} + 60 ))
LANDING="$HDFS_BASE/landing/timesheet_stream"
mkdir -p "$ROOT/logs"
TS="$(date +%Y%m%d_%H%M%S)"

echo "### Resetting landing folder, checkpoint and stream output"
hdfs dfs -rm -r -f -skipTrash "$LANDING" "$HDFS_BASE/landing/_checkpoints/timesheet_stream" \
                              "$HDFS_BASE/bronze/stream/timesheet" >/dev/null 2>&1 || true
hdfs dfs -mkdir -p "$LANDING"

echo "### Starting Spark streaming consumer (runs ${RUN_SECONDS}s)"
spark-submit --master "$SPARK_MASTER" "$ROOT/streaming/stream_ingest.py" "hdfs://$HDFS_BASE" "$RUN_SECONDS" \
  > "$ROOT/logs/streaming_consumer_$TS.log" 2>&1 &
CONSUMER=$!
sleep 25   # give Spark time to start

echo "### Replaying timesheets: $BATCHES batches x $ROWS rows, every ${PAUSE}s"
python3 "$ROOT/streaming/replay_timesheets.py" "$STAGING_DIR/Timesheet.csv" "$LANDING" \
        "$ROWS" "$PAUSE" "$BATCHES" | tee "$ROOT/logs/streaming_producer_$TS.log"

echo "### Waiting for the consumer to finish"
wait "$CONSUMER" || true

echo
echo "### Consumer output (batches processed)"
grep -E "^\[batch|Streaming job" "$ROOT/logs/streaming_consumer_$TS.log" || \
  echo "No batch lines found - see $ROOT/logs/streaming_consumer_$TS.log"
echo
echo "### Files landed and written"
hdfs dfs -ls "$LANDING" | tail -n +2 | wc -l | xargs echo "landing files:"
hdfs dfs -ls "$HDFS_BASE/bronze/stream/timesheet" | head
