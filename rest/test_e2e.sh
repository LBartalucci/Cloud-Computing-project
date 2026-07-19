#!/usr/bin/env bash
# End-to-end smoke test of the REST gateway (run on the namenode).
set -e
S=http://localhost:5000
ENGINE=${1:-hadoop}
DATASET=${2:-dev}
PRODUCT=${3:-1004856}

echo "--- health ---"; curl -s $S/health; echo
echo "--- submit $ENGINE on $DATASET ---"
JOB=$(curl -s -XPOST $S/jobs -H 'content-type: application/json' \
      -d "{\"engine\":\"$ENGINE\",\"dataset\":\"$DATASET\",\"topn\":10}" \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
echo "job=$JOB"
for i in $(seq 1 90); do
  sleep 5
  ST=$(curl -s $S/jobs/$JOB | python3 -c 'import sys,json;print(json.load(sys.stdin)["state"])')
  echo "  poll $i: $ST"
  case "$ST" in SUCCEEDED|FAILED) break;; esac
done
echo "--- job detail ---"; curl -s $S/jobs/$JOB | python3 -m json.tool
echo "--- recommendations for $PRODUCT ---"; curl -s $S/jobs/$JOB/recommendations/$PRODUCT | python3 -m json.tool
