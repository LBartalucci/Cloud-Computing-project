#!/usr/bin/env python3
"""Render benchmark.csv into Markdown tables for the report.

Usage: python scripts/make_tables.py [benchmark.csv]
Prints Markdown to stdout (scaling, reducers, partitions tables).
"""
import csv
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "../results/benchmark.csv"

rows = []
with open(path, newline="") as f:
    for r in csv.DictReader(f):
        rows.append(r)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def scaling_table():
    by = defaultdict(dict)  # size -> engine -> wall
    extra = {}
    for r in rows:
        if r["experiment"] != "scaling":
            continue
        by[int(r["size_mb"])][r["engine"]] = r["wall_s"]
        if r["engine"] == "baseline":
            extra[int(r["size_mb"])] = (r.get("peak_rss_mb", ""), r.get("sessions", ""),
                                        r.get("pairs", ""), r.get("anchors", ""))
    out = ["### Execution time vs dataset size (seconds)\n",
           "| Size (MB) | Hadoop | Spark | Python baseline | baseline peak RSS (MB) |",
           "|---:|---:|---:|---:|---:|"]
    for s in sorted(by):
        h = by[s].get("hadoop", "-")
        sp = by[s].get("spark", "-")
        b = by[s].get("baseline", "-")
        rss = extra.get(s, ("", "", "", ""))[0]
        out.append("| %d | %s | %s | %s | %s |" % (s, h, sp, b, rss))
    return "\n".join(out)


def sweep_table(exp, engine, label):
    out = ["\n### %s\n" % label,
           "| %s | wall (s) |" % engine,
           "|---:|---:|"]
    for r in rows:
        if r["experiment"] == exp and r["engine"] == engine.split()[0].lower():
            out.append("| %s | %s |" % (r["param"], r["wall_s"]))
    return "\n".join(out)


print(scaling_table())
print(sweep_table("reducers", "Hadoop reducers", "Hadoop: effect of reducer count (1500 MB)"))
print(sweep_table("partitions", "Spark partitions", "Spark: effect of partition count (1500 MB)"))