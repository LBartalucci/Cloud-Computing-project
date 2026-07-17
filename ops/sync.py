#!/usr/bin/env python3
"""Recursively upload a local directory tree to a remote dir via SFTP.

Usage:
  python ops/sync.py <host> <localdir> <remotedir>

Skips common build/VCS noise (.git, target, __pycache__, *.pyc). Creates remote
directories as needed. Reuses the host aliases and credentials from vm.py.
"""
import os
import sys
import stat

import paramiko

from vm import HOSTS, USER, PASS  # noqa: E402  (sibling module)

SKIP_DIRS = {".git", "target", "__pycache__", ".idea", ".mvn", "node_modules"}
SKIP_EXT = {".pyc", ".class"}


def ensure_remote_dir(sftp, path):
    parts = path.strip("/").split("/")
    cur = ""
    for p in parts:
        cur = cur + "/" + p
        try:
            sftp.stat(cur)
        except IOError:
            sftp.mkdir(cur)


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        return 2
    host, localdir, remotedir = sys.argv[1], sys.argv[2], sys.argv[3]
    ip = HOSTS.get(host, host)

    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(ip, username=USER, password=PASS, timeout=20,
                look_for_keys=False, allow_agent=False)
    sftp = cli.open_sftp()

    localdir = os.path.abspath(localdir)
    count = 0
    for root, dirs, files in os.walk(localdir):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        rel = os.path.relpath(root, localdir).replace("\\", "/")
        rdir = remotedir if rel == "." else remotedir + "/" + rel
        ensure_remote_dir(sftp, rdir)
        for f in files:
            if os.path.splitext(f)[1] in SKIP_EXT:
                continue
            lpath = os.path.join(root, f)
            rpath = rdir + "/" + f
            sftp.put(lpath, rpath)
            count += 1
            print("PUT", rpath)
    print("SYNCED", count, "files ->", host + ":" + remotedir)
    sftp.close()
    cli.close()


if __name__ == "__main__":
    sys.exit(main())
