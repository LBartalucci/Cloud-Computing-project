#!/usr/bin/env bash
# Run the PySpark recommender on YARN.

# Usage: run_spark.sh <hdfs_input> <hdfs_output> [event_types] [topN] [num_parts] [executors]
set -eo pipefail

source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH=$PATH:$SPARK_HOME/bin

APP=${APP:-/home/hadoop/cc_build/spark/reco_spark.py}
IN=${1:?input}; OUT=${2:?output}
ETYPES=${3:-cart,purchase}; TOPN=${4:-10}; NPARTS=${5:--}; EXECS=${6:-2}

echo ">>> Cleaning previous output"
hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true

echo ">>> spark-submit: IN=$IN OUT=$OUT events=$ETYPES topN=$TOPN parts=$NPARTS execs=$EXECS"
START=$(date +%s)
set +e
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors "$EXECS" --executor-memory 512m --executor-cores 1 \
  "$APP" "$IN" "$OUT" "$ETYPES" "$TOPN" "$NPARTS"
RC=$?
set -e
END=$(date +%s)
echo ">>> Spark wall-clock: $((END-START))s"

echo ">>> Output sample:"
hdfs dfs -ls "$OUT" 2>/dev/null || true
{ hdfs dfs -cat "$OUT/part-00000" 2>/dev/null | head -10; } || true

exit $RC
