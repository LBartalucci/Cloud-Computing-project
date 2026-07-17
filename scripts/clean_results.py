#!/usr/bin/env python3
"""Rebuild a clean benchmark CSV from the harness '[done]' log lines."""

import glob
import os
import re

# define paths
RES = "/home/hadoop/cc_build/results"
LOGDIR = RES + "/logs"
OUTS = [RES + "/benchmark.out", RES + "/benchmark_sweeps.out"]

# regex to parse log lines for different engines
H = re.compile(r"\[done\] hadoop exp=(\S+) size=(\d+) red=(\S+) wall=([\d.]+)s cpu=(\d+)ms mem=(\d+)MB rc=(\d+)")
S = re.compile(r"\[done\] spark exp=(\S+) size=(\d+) parts=(\S+) wall=([\d.]+)s rc=(\d+)")
B = re.compile(r"\[done\] baseline exp=(\S+) size=(\d+) wall=([\d.]+)s rss=(\d+)MB rc=(\d+)")
BSTATS = re.compile(r"sessions=(\d+) pairs=(\d+) anchors=(\d+)")

rows = []

def baseline_stats(size):
    # get stats from baseline log file
    p = os.path.join(LOGDIR, "b_%s.log" % size)
    if os.path.exists(p):
        with open(p) as f:
            m = BSTATS.search(f.read())
            if m:
                return m.group(1), m.group(2), m.group(3)
    return "", "", ""

# process log files
for path in OUTS:
    if not os.path.exists(path):
        continue
    with open(path) as f:
        for line in f:
            # parse hadoop results
            m = H.search(line)
            if m:
                exp, size, red, wall, cpu, mem, rc = m.groups()
                rows.append([exp, "hadoop", size, "reducers=" + red, wall, cpu, mem, "", "", "", "", rc])
                continue
            # parse spark results
            m = S.search(line)
            if m:
                exp, size, parts, wall, rc = m.groups()
                rows.append([exp, "spark", size, "parts=" + parts, wall, "", "", "", "", "", "", rc])
                continue
            # parse baseline results
            m = B.search(line)
            if m:
                exp, size, wall, rss, rc = m.groups()
                ses, pai, anc = baseline_stats(size)
                rows.append([exp, "baseline", size, "", wall, "", "", rss, ses, pai, anc, rc])

# print header
print("experiment,engine,size_mb,param,wall_s,cpu_ms,phys_mem_mb,peak_rss_mb,sessions,pairs,anchors,exit")
# print rows
for r in rows:
    print(",".join(r))