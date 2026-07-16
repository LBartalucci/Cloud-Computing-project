#!/usr/bin/env bash
# Build intermediate size slices on the VM from the already-present main CSV and
# load every scaling input into HDFS under /user/hadoop/ecommerce/sizes/<tag>.
set -eo pipefail
source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export HADOOP_ROOT_LOGGER=WARN,console

DATA=/home/hadoop/data
SRC=$DATA/2019-Oct-main.csv
SCRIPTS=/home/hadoop/cc_build/scripts

# 500 MB and 1 GB slices (dev=150MB and main=1.5GB already exist locally).
python3 "$SCRIPTS/make_slices.py" "$SRC" "$DATA" \
    s500=500MB s1000=1000MB s2000=2000MB s2500=2500MB

declare -A FILES=(
  [150MB]=$DATA/2019-Oct-dev.csv
  [500MB]=$DATA/2019-Oct-s500.csv
  [1000MB]=$DATA/2019-Oct-s1000.csv
  [1500MB]=$DATA/2019-Oct-main.csv
  [2000MB]=$DATA/2019-Oct-s2000.csv
  [2500MB]=$DATA/2019-Oct-s2500.csv
)

for tag in 150MB 500MB 1000MB 1500MB 2000MB 2500MB; do
  f=${FILES[$tag]}
  echo ">>> HDFS load $tag <- $f ($(du -h "$f" | cut -f1))"
  hdfs dfs -mkdir -p /user/hadoop/ecommerce/sizes/$tag
  hdfs dfs -put -f "$f" /user/hadoop/ecommerce/sizes/$tag/
done

echo ">>> sizes in HDFS:"
hdfs dfs -ls -h -R /user/hadoop/ecommerce/sizes | grep -E '\.csv$'