#!/usr/bin/env bash
# Run the Hadoop cascade (now containing IMC) on the cluster.
#
# Usage: run_hadoop.sh <hdfs_input> <hdfs_output> [num_reducers] [event_types] [topN]
source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER:-WARN,console}

set -eo pipefail

JAR=${JAR:-/home/hadoop/cc_build/hadoop/target/ecommerce-reco.jar}
IN=${1:?input}; OUT=${2:?output}
NRED=${3:-2}; ETYPES=${4:-cart,purchase}; TOPN=${5:-10}

echo ">>> Cleaning previous output"
hdfs dfs -rm -r -f "$OUT" "${OUT}_stage1" >/dev/null 2>&1 || true

echo ">>> Running cascade (IMC): IN=$IN OUT=$OUT reducers=$NRED events=$ETYPES topN=$TOPN"
START=$(date +%s)
set +e
hadoop jar "$JAR" it.unipi.hadoop.RecoDriver \
  -Dcc.event.types="$ETYPES" -Dcc.topn="$TOPN" \
  "$IN" "$OUT" "$NRED"
RC=$?
set -e
END=$(date +%s)
echo ">>> Cascade wall-clock: $((END-START))s"

# Best-effort sample usando il carattere jolly per aggregare i risultati di tutte le partizioni
echo ">>> Output sample:"
hdfs dfs -ls "$OUT" 2>/dev/null || true
{ hdfs dfs -cat "$OUT/part-r-*" 2>/dev/null | head -10; } || true

exit $RC