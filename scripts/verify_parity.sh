#!/usr/bin/env bash
# Verify that the modified Hadoop (IMC) and Spark implementations produce identical results
# on a given HDFS input. Re-runs the Hadoop cascade, then merges+sorts both
# outputs and diffs them.
source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null || true
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_ROOT_LOGGER=WARN,console

INPUT=${INPUT:-/user/hadoop/ecommerce/dev}
HOUT=${HOUT:-/user/hadoop/ecommerce/out_dev}
SOUT=${SOUT:-/user/hadoop/ecommerce/spark_dev}

# Esegue la suite Hadoop (che in questa repo è la versione IMC)
bash /home/hadoop/cc_build/scripts/run_hadoop.sh "$INPUT" "$HOUT" 2 cart,purchase 10 > /tmp/had_imc.log 2>&1
echo "HADOOP_RC=$?"
tail -2 /tmp/had_imc.log

# Raccoglie e ordina gli output per il confronto deterministico
hdfs dfs -cat "$HOUT"/part-r-* 2>/dev/null | sort > /tmp/h_imc.txt
hdfs dfs -cat "$SOUT"/part-* 2>/dev/null | sort > /tmp/s.txt

hl=$(wc -l < /tmp/h_imc.txt); sl=$(wc -l < /tmp/s.txt)
echo "hadoop_imc_lines=$hl spark_lines=$sl"

diff /tmp/h_imc.txt /tmp/s.txt > /tmp/diff.txt 2>&1
dl=$(wc -l < /tmp/diff.txt)
echo "diff_lines=$dl"

if [ "$dl" -eq 0 ]; then 
  echo "PARITY_OK: Hadoop (IMC) == Spark"; 
else 
  echo "!!! PARITY_MISMATCH (first 20 diff lines):"; 
  head -20 /tmp/diff.txt; 
fi