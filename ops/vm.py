#!/usr/bin/env python3
"""Tiny non-interactive SSH/SFTP helper for driving the two project VMs.

Usage:
  python ops/vm.py <host> exec  "<remote command>"        # run, stream output, propagate exit code
  python ops/vm.py <host> exec  -                          # read remote command/script from stdin
  python ops/vm.py <host> put   <local> <remote>           # upload a file (SFTP)
  python ops/vm.py <host> get   <remote> <local>           # download a file (SFTP)
  python ops/vm.py <host> hadoop "<cmd>"                   # run as the 'hadoop' login user

<host> may be an alias (nn/namenode/156, dn/datanode/150) or a literal IP/hostname.

Auth: password-based (lab VMs share password 'ubuntu'); host keys auto-accepted.
Override via env CC_VM_USER / CC_VM_PASS if needed.
"""
import os
import sys

import paramiko

HOSTS = {
    "nn": "10.1.1.156", "namenode": "10.1.1.156", "156": "10.1.1.156",
    "dn": "10.1.1.150", "datanode": "10.1.1.150", "datanode-2": "10.1.1.150", "150": "10.1.1.150",
}

USER = os.environ.get("CC_VM_USER", "root")
PASS = os.environ.get("CC_VM_PASS", "ubuntu")


def connect(host):
    ip = HOSTS.get(host, host)
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(ip, username=USER, password=PASS, timeout=20,
                look_for_keys=False, allow_agent=False, banner_timeout=20)
    return cli


def run(cli, command):
    """Run a command, stream stdout/stderr live, return remote exit code."""
    chan = cli.get_transport().open_session()
    chan.settimeout(None)
    chan.exec_command(command)
    chan.shutdown_write()
    while True:
        done = chan.exit_status_ready()
        while chan.recv_ready():
            sys.stdout.write(chan.recv(65536).decode("utf-8", "replace"))
            sys.stdout.flush()
        while chan.recv_stderr_ready():
            sys.stderr.write(chan.recv_stderr(65536).decode("utf-8", "replace"))
            sys.stderr.flush()
        if done and not chan.recv_ready() and not chan.recv_stderr_ready():
            break
    return chan.recv_exit_status()


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    host, action = sys.argv[1], sys.argv[2]
    cli = connect(host)
    try:
        if action == "exec":
            cmd = sys.stdin.read() if sys.argv[3] == "-" else sys.argv[3]
            return run(cli, cmd)
        if action == "hadoop":
            inner = sys.stdin.read() if sys.argv[3] == "-" else sys.argv[3]
            # Run under the hadoop login user with a login shell.
            safe = inner.replace("'", "'\\''")
            return run(cli, "su - hadoop -c 'bash -lc '\\''" + safe + "'\\'''")
        if action == "put":
            sftp = cli.open_sftp()
            sftp.put(sys.argv[3], sys.argv[4])
            print("PUT_OK", sys.argv[3], "->", sys.argv[4])
            return 0
        if action == "get":
            sftp = cli.open_sftp()
            sftp.get(sys.argv[3], sys.argv[4])
            print("GET_OK", sys.argv[3], "->", sys.argv[4])
            return 0
        print("unknown action:", action)
        return 2
    finally:
        cli.close()


if __name__ == "__main__":
    sys.exit(main())
