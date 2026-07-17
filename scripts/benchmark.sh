#!/usr/bin/env bash
# Experimental evaluation harness. Runs the three engines across dataset sizes
# and configuration sweeps, timing each run and parsing resource counters.
# Results are appended incrementally to a CSV so partial progress is visible.
#
# Experiments:
#   scaling      : {hadoop,spark,baseline} x {150,500,1000,1500} MB
#   reducers     : hadoop on 1500 MB, reducers in {1,2,4}
#   partitions   : spark  on 1500 MB, partitions in {2,4,8}
# Source cluster env BEFORE strict mode (hadoop-env.sh references vars that may
# be unbound). No -e/-u here: each run captures its own exit code.
source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH=$PATH:$SPARK_HOME/bin
set -o pipefail

JAR=/home/hadoop/cc_build/hadoop/target/ecommerce-reco.jar
SPARK_APP=/home/hadoop/cc_build/spark/reco_spark.py
BASELINE=/home/hadoop/cc_build/python_baseline/reco_baseline.py
DATA=/home/hadoop/data
OUTBASE=/user/hadoop/ecommerce/bench
RESULTS=${RESULTS:-/home/hadoop/cc_build/results/benchmark.csv}
LOGDIR=/home/hadoop/cc_build/results/logs
mkdir -p "$(dirname "$RESULTS")" "$LOGDIR"

declare -A HDFS_IN=(
  [500]=/user/hadoop/ecommerce/sizes/500MB
  [1000]=/user/hadoop/ecommerce/sizes/1000MB
  [1500]=/user/hadoop/ecommerce/sizes/1500MB
  [2000]=/user/hadoop/ecommerce/sizes/2000MB
  [2500]=/user/hadoop/ecommerce/sizes/2500MB )

declare -A LOCAL_IN=(
  [500]=$DATA/2019-Oct-s500.csv
  [1000]=$DATA/2019-Oct-s1000.csv
  [1500]=$DATA/2019-Oct-main.csv
  [2000]=$DATA/2019-Oct-s2000.csv
  [2500]=$DATA/2019-Oct-s2500.csv )

if [ ! -f "$RESULTS" ]; then
  echo "experiment,engine,size_mb,param,wall_s,cpu_ms,phys_mem_mb,peak_rss_mb,sessions,pairs,anchors,exit" > "$RESULTS"
fi

run_hadoop() {  # experiment size reducers
  local exp=$1 size=$2 red=$3
  local out=$OUTBASE/h_${size}_${red} log=$LOGDIR/h_${size}_${red}.log
  hdfs dfs -rm -r -f "$out" "${out}_stage1" >/dev/null 2>&1
  local t0 t1 wall rc cpu mem
  t0=$(date +%s.%N)
  HADOOP_ROOT_LOGGER=INFO,console hadoop jar "$JAR" it.unipi.hadoop.RecoDriver \
      -Dcc.event.types=cart,purchase -Dcc.topn=10 "${HDFS_IN[$size]}" "$out" "$red" \
      > "$log" 2>&1
  rc=$?
  t1=$(date +%s.%N)
  wall=$(awk "BEGIN{printf \"%.1f\", $t1-$t0}")
  cpu=$(grep -a "CPU time spent (ms)" "$log" | awk -F= '{s+=$2} END{print s+0}')
  mem=$(grep -a "Physical memory (bytes) snapshot" "$log" | awk -F= '{if($2+0>m)m=$2} END{printf "%d", m/1048576}')
  echo "$exp,hadoop,$size,reducers=$red,$wall,$cpu,$mem,,,,$rc" >> "$RESULTS"
  echo "[done] hadoop exp=$exp size=$size red=$red wall=${wall}s cpu=${cpu}ms mem=${mem}MB rc=$rc"
}

run_spark() {  # experiment size partitions
  local exp=$1 size=$2 parts=$3
  local out=$OUTBASE/s_${size}_${parts} log=$LOGDIR/s_${size}_${parts}.log
  hdfs dfs -rm -r -f "$out" >/dev/null 2>&1
  local t0 t1 wall rc
  t0=$(date +%s.%N)
  spark-submit --master yarn --deploy-mode client \
      --num-executors 2 --executor-memory 512m --executor-cores 1 \
      "$SPARK_APP" "${HDFS_IN[$size]}" "$out" cart,purchase 10 "$parts" \
      > "$log" 2>&1
  rc=$?
  t1=$(date +%s.%N)
  wall=$(awk "BEGIN{printf \"%.1f\", $t1-$t0}")
  echo "$exp,spark,$size,parts=$parts,$wall,,,,,,$rc" >> "$RESULTS"
  echo "[done] spark exp=$exp size=$size parts=$parts wall=${wall}s rc=$rc"
}

run_baseline() {  # experiment size
  local exp=$1 size=$2
  local out=/tmp/bench_baseline_${size}.txt log=$LOGDIR/b_${size}.log
  local t0 t1 wall rc rss tot ses pai anc
  t0=$(date +%s.%N)
  python3 "$BASELINE" "${LOCAL_IN[$size]}" "$out" cart,purchase 10 > "$log" 2>&1
  rc=$?
  t1=$(date +%s.%N)
  wall=$(awk "BEGIN{printf \"%.1f\", $t1-$t0}")
  # Parse the single summary line ("... sessions=.. pairs=.. anchors=..") to
  # avoid matching the timing line which also contains "pairs=".
  local sum
  sum=$(grep -a "rows=" "$log" | head -1)
  rss=$(grep -a "peakRSS=" "$log" | head -1 | sed -E 's/.*peakRSS=([0-9]+)MB.*/\1/')
  ses=$(echo "$sum" | sed -E 's/.*sessions=([0-9]+).*/\1/')
  pai=$(echo "$sum" | sed -E 's/.* pairs=([0-9]+).*/\1/')
  anc=$(echo "$sum" | sed -E 's/.*anchors=([0-9]+).*/\1/')
  echo "$exp,baseline,$size,,$wall,,,${rss:-},${ses:-},${pai:-},${anc:-},$rc" >> "$RESULTS"
  echo "[done] baseline exp=$exp size=$size wall=${wall}s rss=${rss}MB rc=$rc"
}

echo "=== START $(date) ==="

echo "--- Experiment A: scaling ---"

for s in 500 1000 1500 2000 2500; do
  run_hadoop scaling "$s" 2
  run_spark  scaling "$s" -
  run_baseline scaling "$s"
done

echo "--- Experiment B: hadoop reducers (1500MB) ---"
for r in 1 2 4; do run_hadoop reducers 1500 "$r"; done

echo "--- Experiment C: spark partitions (1500MB) ---"
for p in 2 4 8; do run_spark partitions 1500 "$p"; done

echo "=== DONE $(date) ==="
echo "=== RESULTS ==="
cat "$RESULTS"
