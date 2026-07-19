# REST gateway

A containerized Flask service that is the **intermediate layer between the
client and the distributed backend**. It submits Hadoop / Spark / baseline jobs
to the cluster over SSH and exposes asynchronous status + recommendation
queries. The image stays tiny (Python + Flask + paramiko) — it does not bundle
Hadoop.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET  | `/health` | liveness probe |
| POST | `/jobs` | submit a job (async) → `202 {job_id}` |
| GET  | `/jobs` | list jobs |
| GET  | `/jobs/{id}` | status, timing, parsed stats |
| GET  | `/jobs/{id}/log` | captured job log |
| GET  | `/jobs/{id}/recommendations/{product}?limit=N` | top-N co-occurring products |
| GET  | `/openapi.yaml` | OpenAPI 3 spec |

`POST /jobs` body:

```json
{ "engine": "hadoop|spark|baseline", "dataset": "dev|main",
  "event_types": "cart,purchase", "topn": 10, "reducers": 2 }
```

## Run (on the namenode)

```bash
cd /home/hadoop/cc_build/rest
docker compose up -d --build
curl -s localhost:5000/health
```

## Use the client (from anywhere that can reach the server)

```bash
pip install -r client/requirements.txt
python client/reco_client.py --server http://10.1.1.156:5000 \
    --engine spark --dataset dev --product 1004856
```

## Example with curl

```bash
JOB=$(curl -s -XPOST localhost:5000/jobs -H 'content-type: application/json' \
      -d '{"engine":"hadoop","dataset":"dev","topn":10}' | jq -r .job_id)
curl -s localhost:5000/jobs/$JOB | jq
curl -s localhost:5000/jobs/$JOB/recommendations/1004856 | jq
```
