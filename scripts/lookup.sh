#!/usr/bin/env bash
# Look up the recommendation line for a single anchor product.
# Usage: lookup.sh <hdfs|local> <src> <engine> <product>
#   hdfs : src = HDFS output dir, engine selects the part-file glob
#   local: src = local file (baseline output)
source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export HADOOP_ROOT_LOGGER=WARN,console

MODE=$1; SRC=$2; ENGINE=$3; PROD=$4

if [ "$MODE" = "local" ]; then
  awk -F'\t' -v p="$PROD" '$1==p{print; exit}' "$SRC"
else
  case "$ENGINE" in
    hadoop) P='part-r-*' ;;
    *)      P='part-*' ;;
  esac
  hdfs dfs -cat "$SRC/$P" 2>/dev/null | awk -F'\t' -v p="$PROD" '$1==p{print; exit}'
fi
