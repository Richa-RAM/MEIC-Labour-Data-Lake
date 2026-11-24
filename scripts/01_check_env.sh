#!/usr/bin/env bash
# =====================================================================
#  STEP 1 - Environment check (screenshot evidence for the report)
#  Shows the running Java services (jps), tool versions, HDFS health
#  and the web UI addresses.
# =====================================================================
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/config.env"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin:$PATH"

echo "### Install folders used (from config.env)"
for v in HADOOP_HOME SPARK_HOME HIVE_HOME ZEPPELIN_HOME; do
  if [ -d "${!v}" ]; then echo "  [OK]      $v=${!v}"; else echo "  [MISSING] $v=${!v}  -> fix in config.env"; fi
done
command -v spark-submit >/dev/null || echo "  [MISSING] spark-submit not found - Spark may only exist inside Zeppelin"
command -v beeline >/dev/null || echo "  [MISSING] beeline not found - set HIVE_MODE=hive in config.env"
echo

echo "### Running services (jps)"
jps
echo
for svc in NameNode DataNode SecondaryNameNode ResourceManager NodeManager; do
  if jps | grep -qw "$svc"; then echo "  [OK]      $svc"; else echo "  [MISSING] $svc  -> run scripts/00_start_services.sh"; fi
done
pgrep -f HiveMetaStore >/dev/null && echo "  [OK]      Hive Metastore"  || echo "  [info]    Hive Metastore not separate (fine if HiveServer2 is OK)"
pgrep -f HiveServer2   >/dev/null && echo "  [OK]      HiveServer2"     || echo "  [MISSING] HiveServer2"

echo
echo "### Versions"
hadoop version 2>/dev/null | head -1
spark-submit --version 2>&1 | grep -m1 -i "version"
hive --version 2>/dev/null | head -1
python3 --version

echo
echo "### HDFS summary"
hdfs dfsadmin -report 2>/dev/null | grep -E "Configured Capacity|DFS Used:|Live datanodes" | head -3

echo
echo "### Web UIs (open in the VM browser for screenshots)"
echo "  HDFS NameNode : http://localhost:50070  (Hadoop 3.x: port 9870)"
echo "  YARN          : http://localhost:8088"
echo "  HiveServer2   : http://localhost:10002"
echo "  Spark job UI  : http://localhost:4040   (only while a job runs)"
echo "  Zeppelin      : http://localhost:8080"
