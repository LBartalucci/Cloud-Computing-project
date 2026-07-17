#!/usr/bin/env python3
"""Stage a local CSV slice into HDFS efficiently over a slow link.

Pipeline: gzip locally -> SFTP the (much smaller) .gz to the namenode ->
gunzip on the VM -> `hdfs dfs -put` the raw CSV (kept splittable for MapReduce).

Usage:
  python scripts/push_dataset.py <local_csv> <hdfs_subdir>

Example:
  python scripts/push_dataset.py data/2019-Oct-main.csv main
  # -> /user/hadoop/ecommerce/main/<file>.csv in HDFS
"""
import gzip
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ops"))
import paramiko  # noqa: E402
from vm import HOSTS, USER, PASS  # noqa: E402

HOST = HOSTS["nn"]
REMOTE_DIR = "/home/hadoop/data"
HDFS_BASE = "/user/hadoop/ecommerce"


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def run(cli, cmd):
    chan = cli.get_transport().open_session()
    chan.exec_command(cmd)
    chan.shutdown_write()
    out = b""
    while True:
        done = chan.exit_status_ready()
        while chan.recv_ready():
            out += chan.recv(65536)
        if done and not chan.recv_ready():
            break
    rc = chan.recv_exit_status()
    return rc, out.decode("utf-8", "replace")


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    local_csv, subdir = sys.argv[1], sys.argv[2]
    base = os.path.basename(local_csv)
    gz_local = local_csv + ".gz"

    if not os.path.exists(gz_local):
        log("gzip %s -> %s" % (local_csv, gz_local))
        with open(local_csv, "rb") as fin, gzip.open(gz_local, "wb", compresslevel=1) as fout:
            shutil.copyfileobj(fin, fout, length=8 * 1024 * 1024)
    log("local gz size: %.1f MB" % (os.path.getsize(gz_local) / 1024**2))

    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(HOST, username=USER, password=PASS, timeout=20,
                look_for_keys=False, allow_agent=False)

    run(cli, "mkdir -p %s && chown hadoop:hadoopgroup %s" % (REMOTE_DIR, REMOTE_DIR))
    remote_gz = REMOTE_DIR + "/" + base + ".gz"

    log("SFTP upload -> %s" % remote_gz)
    t0 = time.time()
    sftp = cli.open_sftp()
    sftp.put(gz_local, remote_gz)
    sftp.close()
    log("upload done in %.0fs" % (time.time() - t0))

    log("gunzip + hdfs put on VM")
    cmd = (
        "su - hadoop -c 'bash -lc \""
        "source /home/hadoop/cc_ops/hadoop-env.sh 2>/dev/null; "
        "export HADOOP_ROOT_LOGGER=WARN,console; "
        "gunzip -f " + remote_gz + " && "
        "hdfs dfs -mkdir -p " + HDFS_BASE + "/" + subdir + " && "
        "hdfs dfs -put -f " + REMOTE_DIR + "/" + base + " " + HDFS_BASE + "/" + subdir + "/ && "
        "hdfs dfs -ls -h " + HDFS_BASE + "/" + subdir + "\"'"
    )
    rc, out = run(cli, cmd)
    print(out)
    cli.close()
    log("DONE rc=%d" % rc)
    return rc


if __name__ == "__main__":
    sys.exit(main())
