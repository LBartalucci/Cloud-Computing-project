#!/usr/bin/env python3
"""
Non-parallel, single-process baseline for the co-occurrence recommender.

Same logic as the Hadoop/Spark jobs, implemented with simple Python data
structures on one machine. Used as the reference point in the experimental
comparison (does not scale: the whole co-occurrence map lives in RAM).

Reads a local CSV file (not HDFS) and writes a single output file whose format
matches the distributed jobs:  anchor<TAB>other1:count1,other2:count2,...

Usage:
  python reco_baseline.py <local_csv_in> <local_out> [event_types] [topN]

Prints wall-clock time and peak RSS (on Linux) to stderr for benchmarking.
"""
import sys
import time
from collections import defaultdict
from itertools import combinations

IDX_EVENT_TYPE = 1
IDX_PRODUCT_ID = 2
IDX_USER_SESSION = 8
MIN_FIELDS = 9
MAX_SESSION_SIZE = 50


def peak_rss_mb():
    try:
        import resource
        # ru_maxrss is KB on Linux, bytes on macOS.
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024.0
    except Exception:
        return float("nan")


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(__doc__)
        return 2
    in_path, out_path = sys.argv[1], sys.argv[2]
    event_types = set(
        t.strip().lower()
        for t in (sys.argv[3] if len(sys.argv) > 3 else "cart,purchase").split(",")
        if t.strip()
    )
    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 10

    t0 = time.time()

    # Stage 1a: reconstruct session baskets (distinct products per session).
    sessions = defaultdict(set)
    rows = kept = 0
    with open(in_path, "r", encoding="utf-8", newline="") as f:
        first = True
        for line in f:
            if first:
                first = False
                if line.startswith("event_time"):
                    continue
            rows += 1
            fld = line.rstrip("\n").split(",")
            if len(fld) < MIN_FIELDS:
                continue
            if fld[IDX_EVENT_TYPE].strip().lower() not in event_types:
                continue
            session = fld[IDX_USER_SESSION].strip()
            prod = fld[IDX_PRODUCT_ID].strip()
            if not session or not prod:
                continue
            try:
                sessions[session].add(int(prod))
            except ValueError:
                continue
            kept += 1
    t_parse = time.time()

    # Stage 1b: enumerate co-occurring pairs and count sessions per pair.
    pair_counts = defaultdict(int)
    for prods in sessions.values():
        n = len(prods)
        if n < 2 or n > MAX_SESSION_SIZE:
            continue
        for a, b in combinations(sorted(prods), 2):
            pair_counts[(a, b)] += 1
    t_pairs = time.time()

    # Stage 2: both directions, then top-N per anchor.
    anchor = defaultdict(list)
    for (a, b), c in pair_counts.items():
        anchor[a].append((b, c))
        anchor[b].append((a, c))

    with open(out_path, "w", encoding="utf-8") as out:
        for a in sorted(anchor):
            top = sorted(anchor[a], key=lambda x: (-x[1], x[0]))[:top_n]
            out.write("%d\t%s\n" % (a, ",".join("%d:%d" % (o, c) for o, c in top)))
    t_end = time.time()

    sys.stderr.write(
        "[baseline] rows=%d kept_events=%d sessions=%d pairs=%d anchors=%d\n"
        % (rows, kept, len(sessions), len(pair_counts), len(anchor))
    )
    sys.stderr.write(
        "[baseline] parse=%.1fs pairs=%.1fs rank+write=%.1fs TOTAL=%.1fs peakRSS=%.0fMB\n"
        % (t_parse - t0, t_pairs - t_parse, t_end - t_pairs, t_end - t0, peak_rss_mb())
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
