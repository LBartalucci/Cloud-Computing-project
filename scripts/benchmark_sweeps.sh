#!/usr/bin/env bash
# Configuration sweeps only (Experiments B and C), appended to the same results.
# Kept separate so it can be run without redoing the scaling matrix.
source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH=$PATH:$SPARK_HOME/bin
set -o pipefail

JAR=/home/hadoop/cc_build/hadoop/target/ecommerce-reco.jar
SPARK_APP=/home/hadoop/cc_build/spark/reco_spark.py
OUTBASE=/user/hadoop/ecommerce/bench
RESULTS=${RESULTS:-/home/hadoop/cc_build/results/benchmark.csv}
LOGDIR=/home/hadoop/cc_build/results/logs
mkdir -p "$(dirname "$RESULTS")" "$LOGDIR"
IN1500=/user/hadoop/ecommerce/main

run_hadoop() {  # experiment reducers
  local exp=$1 red=$2
  local out=$OUTBASE/h_1500_${red} log=$LOGDIR/h_1500_${red}.log
  hdfs dfs -rm -r -f "$out" "${out}_stage1" >/dev/null 2>&1
  local t0 t1 wall rc cpu mem
  t0=$(date +%s.%N)
  HADOOP_ROOT_LOGGER=INFO,console hadoop jar "$JAR" it.unipi.hadoop.RecoDriver \
      -Dcc.event.types=cart,purchase -Dcc.topn=10 "$IN1500" "$out" "$red" > "$log" 2>&1
  rc=$?
  t1=$(date +%s.%N)
  wall=$(awk "BEGIN{printf \"%.1f\", $t1-$t0}")
  cpu=$(grep -a "CPU time spent (ms)" "$log" | awk -F= '{s+=$2} END{print s+0}')
  mem=$(grep -a "Physical memory (bytes) snapshot" "$log" | awk -F= '{if($2+0>m)m=$2} END{printf "%d", m/1048576}')
  echo "$exp,hadoop,1500,reducers=$red,$wall,$cpu,$mem,,,,$rc" >> "$RESULTS"
  echo "[done] hadoop exp=$exp size=1500 red=$red wall=${wall}s cpu=${cpu}ms mem=${mem}MB rc=$rc"
}

run_spark() {  # experiment partitions
  local exp=$1 parts=$2
  local out=$OUTBASE/s_1500_${parts} log=$LOGDIR/s_1500_${parts}.log
  hdfs dfs -rm -r -f "$out" >/dev/null 2>&1
  local t0 t1 wall rc
  t0=$(date +%s.%N)
  spark-submit --master yarn --deploy-mode client \
      --num-executors 2 --executor-memory 512m --executor-cores 1 \
      "$SPARK_APP" "$IN1500" "$out" cart,purchase 10 "$parts" > "$log" 2>&1
  rc=$?
  t1=$(date +%s.%N)
  wall=$(awk "BEGIN{printf \"%.1f\", $t1-$t0}")
  echo "$exp,spark,1500,parts=$parts,$wall,,,,,,$rc" >> "$RESULTS"
  echo "[done] spark exp=$exp size=1500 parts=$parts wall=${wall}s rc=$rc"
}

echo "=== SWEEPS START $(date) ==="
echo "--- Experiment B: hadoop reducers 1500MB ---"
for r in 1 2 4; do run_hadoop reducers "$r"; done
echo "--- Experiment C: spark partitions 1500MB ---"
for p in 2 4 8; do run_spark partitions "$p"; done
echo "=== SWEEPS DONE $(date) ==="
