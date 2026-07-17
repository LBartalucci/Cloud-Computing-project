#!/usr/bin/env python3
"""Render benchmark.csv into Markdown tables for the report (IMC repository compatible).

Usage: python scripts/make_tables.py [benchmark.csv]
Prints Markdown to stdout (scaling, reducers, partitions tables).
"""
import csv
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "report/results/benchmark.csv"

rows = []
try:
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append(r)
except FileNotFoundError:
    print(f"Error: file {path} not found.")
    sys.exit(1)


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
           "| Size (MB) | Hadoop (IMC) | Spark | Python baseline | baseline peak RSS (MB) |",
           "|---:|---:|---:|---:|---:|"]
    for s in sorted(by):
        # Cerca 'hadoop_imc' o ricade su 'hadoop' se usate vecchi log
        h = by[s].get("hadoop_imc", by[s].get("hadoop", "-"))
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
        if r["experiment"] == exp:
            # Assicura compatibilità se l'engine nel CSV è segnato come 'hadoop_imc' o 'hadoop'
            target_engine = engine.split()[0].lower()
            if target_engine == "hadoop" and r["engine"] == "hadoop_imc":
                out.append("| %s | %s |" % (r["param"], r["wall_s"]))
            elif r["engine"] == target_engine:
                out.append("| %s | %s |" % (r["param"], r["wall_s"]))
    return "\n".join(out)


print(scaling_table())
print(sweep_table("reducers", "Hadoop reducers", "Hadoop (IMC): effect of reducer count (1500 MB)"))
print(sweep_table("partitions", "Spark partitions", "Spark: effect of partition count (1500 MB)"))