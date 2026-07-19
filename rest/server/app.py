"""Containerized REST server for the e-commerce co-occurrence recommender.

Acts as the intermediate layer between the client and the Hadoop/Spark cluster.
Jobs are submitted asynchronously: POST /jobs returns a job_id immediately and a
background worker drives the cluster; GET /jobs/{id} polls status, timing and
parsed statistics; GET /jobs/{id}/recommendations/{product} returns the top-N
co-occurring products once the job has succeeded.
"""
import os
import re
import threading
import time
import uuid

from flask import Flask, jsonify, request, send_from_directory

import cluster

app = Flask(__name__)

JOBS = {}
LOCK = threading.Lock()

WALL_RE = re.compile(r"wall-clock:\s*(\d+)s")
BASE_TOTAL_RE = re.compile(r"TOTAL=([\d.]+)s")
BASE_RSS_RE = re.compile(r"peakRSS=(\d+)MB")
BASE_STATS_RE = re.compile(r"sessions=(\d+) pairs=(\d+) anchors=(\d+)")
VALID_ENGINES = {"hadoop", "spark", "baseline"}


def _parse_stats(engine, log):
    stats = {}
    m = WALL_RE.search(log)
    if m:
        stats["wall_clock_s"] = int(m.group(1))
    if engine == "baseline":
        m = BASE_TOTAL_RE.search(log)
        if m:
            stats["wall_clock_s"] = float(m.group(1))
        m = BASE_RSS_RE.search(log)
        if m:
            stats["peak_rss_mb"] = int(m.group(1))
        m = BASE_STATS_RE.search(log)
        if m:
            stats["sessions"], stats["pairs"], stats["anchors"] = (
                int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return stats


def _worker(job_id):
    job = JOBS[job_id]
    job["state"] = "RUNNING"
    job["started_at"] = time.time()
    try:
        cmd = cluster.build_command(
            job["engine"], job["dataset"], job["output"],
            job["event_types"], job["topn"], job["reducers"])
        job["command"] = cmd
        rc, out = cluster.run_blocking(cmd, as_hadoop=True)
        job["exit_code"] = rc
        job["log"] = out[-20000:]
        job["stats"] = _parse_stats(job["engine"], out)
        job["state"] = "SUCCEEDED" if rc == 0 else "FAILED"
    except Exception as exc:  # noqa: BLE001
        job["state"] = "FAILED"
        job["error"] = str(exc)
    finally:
        job["finished_at"] = time.time()
        if job.get("started_at"):
            job["duration_s"] = round(job["finished_at"] - job["started_at"], 1)


def _public(job):
    return {k: v for k, v in job.items() if k != "log"}


@app.get("/health")
def health():
    return jsonify(status="ok", hostname=os.uname().nodename, jobs=len(JOBS))


@app.get("/openapi.yaml")
def openapi():
    return send_from_directory(os.path.dirname(__file__), "openapi.yaml",
                               mimetype="application/yaml")


@app.post("/jobs")
def submit_job():
    body = request.get_json(force=True, silent=True) or {}
    engine = str(body.get("engine", "hadoop")).lower()
    dataset = str(body.get("dataset", "dev")).lower()
    if engine not in VALID_ENGINES:
        return jsonify(error="engine must be one of %s" % sorted(VALID_ENGINES)), 400
    if dataset not in cluster.DATASETS:
        return jsonify(error="dataset must be one of %s" % sorted(cluster.DATASETS)), 400

    job_id = uuid.uuid4().hex[:12]
    job = {
        "job_id": job_id,
        "engine": engine,
        "dataset": dataset,
        "event_types": str(body.get("event_types", "cart,purchase")),
        "topn": int(body.get("topn", 10)),
        "reducers": int(body.get("reducers", 2)),
        "output": cluster.HDFS_BASE + "/rest_%s_%s" % (engine, job_id),
        "state": "PENDING",
        "submitted_at": time.time(),
    }
    with LOCK:
        JOBS[job_id] = job
    threading.Thread(target=_worker, args=(job_id,), daemon=True).start()
    return jsonify(job_id=job_id, state="PENDING",
                   links={"self": "/jobs/%s" % job_id}), 202


@app.get("/jobs")
def list_jobs():
    with LOCK:
        return jsonify(jobs=[_public(j) for j in JOBS.values()])


@app.get("/jobs/<job_id>")
def get_job(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify(error="not found"), 404
    return jsonify(_public(job))


@app.get("/jobs/<job_id>/log")
def get_log(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify(error="not found"), 404
    return app.response_class(job.get("log", ""), mimetype="text/plain")


@app.get("/jobs/<job_id>/recommendations/<int:product>")
def recommendations(job_id, product):
    job = JOBS.get(job_id)
    if not job:
        return jsonify(error="not found"), 404
    if job["state"] != "SUCCEEDED":
        return jsonify(error="job not finished", state=job["state"]), 409
    limit = int(request.args.get("limit", job["topn"]))
    recs = cluster.fetch_recommendations(job["engine"], job["output"], product, limit)
    if recs is None:
        return jsonify(product=product, recommendations=[],
                       note="product not found in this job's output"), 404
    return jsonify(product=product, engine=job["engine"], recommendations=recs)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
