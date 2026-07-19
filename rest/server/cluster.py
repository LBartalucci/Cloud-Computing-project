"""
Thin SSH gateway from the REST server to the Hadoop/Spark cluster.

Submits jobs over SSH and handles communication with the distributed cluster.
"""
import os
import shlex

import paramiko

NN_HOST = os.environ.get("CC_NN_HOST", "10.1.1.156")
NN_USER = os.environ.get("CC_VM_USER", "root")
NN_PASS = os.environ.get("CC_VM_PASS", "ubuntu")

SCRIPTS_DIR = os.environ.get("CC_SCRIPTS_DIR", "/home/hadoop/cc_build/scripts")
BASELINE = os.environ.get("CC_BASELINE", "/home/hadoop/cc_build/python_baseline/reco_baseline.py")
HDFS_BASE = os.environ.get("CC_HDFS_BASE", "/user/hadoop/ecommerce")
LOCAL_DATA = os.environ.get("CC_LOCAL_DATA", "/home/hadoop/data")

DATASETS = {
    "dev": (HDFS_BASE + "/dev", LOCAL_DATA + "/2019-Oct-dev.csv"),
    "main": (HDFS_BASE + "/main", LOCAL_DATA + "/2019-Oct-main.csv"),
}


def _connect():
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(NN_HOST, username=NN_USER, password=NN_PASS, timeout=20,
                look_for_keys=False, allow_agent=False)
    return cli


def _as_hadoop(inner):
    safe = inner.replace("'", "'\\''")
    return "su - hadoop -c 'bash -lc '\\''" + safe + "'\\'''"


def run_blocking(inner_cmd, as_hadoop=True):
    cli = _connect()
    try:
        cmd = _as_hadoop(inner_cmd) if as_hadoop else inner_cmd
        chan = cli.get_transport().open_session()
        chan.exec_command(cmd)
        chan.shutdown_write()
        out = b""
        while True:
            done = chan.exit_status_ready()
            while chan.recv_ready():
                out += chan.recv(65536)
            while chan.recv_stderr_ready():
                out += chan.recv_stderr(65536)
            if done and not chan.recv_ready() and not chan.recv_stderr_ready():
                break
        rc = chan.recv_exit_status()
        return rc, out.decode("utf-8", "replace")
    finally:
        cli.close()


def build_command(engine, dataset, output, event_types, topn, reducers):
    if dataset not in DATASETS:
        raise ValueError("unknown dataset: %s" % dataset)
    hdfs_in, local_csv = DATASETS[dataset]
    et = shlex.quote(event_types)
    if engine == "hadoop":
        return "bash %s/run_hadoop.sh %s %s %d %s %d" % (
            SCRIPTS_DIR, hdfs_in, output, reducers, et, topn)
    if engine == "spark":
        return "bash %s/run_spark.sh %s %s %s %d - %d" % (
            SCRIPTS_DIR, hdfs_in, output, et, topn, reducers)
    if engine == "baseline":
        local_out = "/tmp/baseline_%s.txt" % os.path.basename(output)
        return "python3 %s %s %s %s %d" % (BASELINE, local_csv, local_out, et, topn)
    raise ValueError("unknown engine: %s" % engine)


def fetch_recommendations(engine, output, product, limit):
    lookup = SCRIPTS_DIR + "/lookup.sh"
    if engine == "baseline":
        src = "/tmp/baseline_%s.txt" % os.path.basename(output)
        cmd = "bash %s local %s baseline %d" % (lookup, src, product)
    else:
        cmd = "bash %s hdfs %s %s %d" % (lookup, output, engine, product)
    rc, out = run_blocking(cmd, as_hadoop=True)
    out = out.strip()
    if not out or "\t" not in out:
        return None
    # Parses the matching line returned by the HDFS lookup script
    _, rest = out.split("\t", 1)
    items = []
    for tok in rest.split(","):
        if ":" in tok:
            p, c = tok.split(":", 1)
            items.append({"product": int(p), "score": int(c)})
    return items[:limit]