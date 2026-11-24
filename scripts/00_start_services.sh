#!/usr/bin/env bash
# =====================================================================
#  STEP 1a - Start Hadoop, Hive and Zeppelin on the VM.
#  Run this once after booting the VM:   bash scripts/00_start_services.sh
#  Services that are already running (seen in jps) are skipped.
# =====================================================================
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/config.env"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin:$PATH"
mkdir -p "$ROOT/logs"

running() { jps | grep -qw "$1"; }

echo ">> HDFS"
running NameNode || start-dfs.sh
echo ">> YARN"
running ResourceManager || start-yarn.sh

# HDFS starts in safe mode for a few seconds; wait until writes are allowed
hdfs dfsadmin -safemode wait

echo ">> Hive metastore"
# Only start a separate metastore if hive-site.xml points to one (hive.metastore.uris).
# Otherwise HiveServer2 uses its built-in metastore and a second one would lock the Derby database.
if grep -q "hive.metastore.uris" "$HIVE_HOME/conf/hive-site.xml" 2>/dev/null; then
  if ! pgrep -f "HiveMetaStore" >/dev/null; then
    nohup hive --service metastore > "$ROOT/logs/metastore.out" 2>&1 &
    sleep 10
  fi
else
  echo "   not needed (embedded metastore)"
fi

echo ">> HiveServer2 (port 10000)"
if ! pgrep -f "HiveServer2" >/dev/null; then
  nohup hive --service hiveserver2 > "$ROOT/logs/hiveserver2.out" 2>&1 &
  echo "   waiting for HiveServer2 on port 10000 (can take 1-2 minutes)..."
  for i in $(seq 1 24); do
    (echo > /dev/tcp/localhost/10000) >/dev/null 2>&1 && { echo "   HiveServer2 is up"; break; }
    sleep 5
  done
fi

echo ">> Zeppelin"
if [ -x "$ZEPPELIN_HOME/bin/zeppelin-daemon.sh" ]; then
  "$ZEPPELIN_HOME/bin/zeppelin-daemon.sh" status >/dev/null 2>&1 || "$ZEPPELIN_HOME/bin/zeppelin-daemon.sh" start
fi

sleep 5
echo
jps
