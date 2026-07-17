#!/usr/bin/env python3
"""Create size-bounded slices of the REES46 CSV for scaling experiments.

Each slice keeps the CSV header and a prefix of the data rows up to (approximately)
the requested size, cutting only on line boundaries. Because sessions are
interleaved by event_time across the month, a temporal prefix is a valid subset
of the workload (documented in the report).

Usage:
  python scripts/make_slices.py <src_csv> <out_dir> name=SIZE [name=SIZE ...]

Example:
  python scripts/make_slices.py data/2019-Oct.csv data \
      dev=150MB small=500MB main=1.5GB

SIZE accepts KB/MB/GB (binary, 1 MB = 1024*1024 bytes) or a raw byte count.
"""
import os
import sys


def parse_size(s):
    s = s.strip().upper()
    mult = 1
    for suffix, m in (("KB", 1024), ("MB", 1024**2), ("GB", 1024**3)):
        if s.endswith(suffix):
            mult = m
            s = s[: -len(suffix)]
            break
    return int(float(s) * mult)


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    src, out_dir = sys.argv[1], sys.argv[2]
    targets = []
    for spec in sys.argv[3:]:
        name, _, size = spec.partition("=")
        targets.append([name, parse_size(size), None, 0])  # name, limit, fh, written
    targets.sort(key=lambda t: t[1])
    os.makedirs(out_dir, exist_ok=True)

    with open(src, "rb") as fin:
        header = fin.readline()
        for t in targets:
            path = os.path.join(out_dir, "2019-Oct-" + t[0] + ".csv")
            fh = open(path, "wb")
            fh.write(header)
            t[2] = fh
            t[3] = len(header)
            t.append(path)

        remaining = len(targets)
        for line in fin:
            for t in targets:
                if t[2] is not None and t[3] < t[1]:
                    t[2].write(line)
                    t[3] += len(line)
                    if t[3] >= t[1]:
                        t[2].close()
                        t[2] = None
                        remaining -= 1
            if remaining == 0:
                break

    for t in targets:
        if t[2] is not None:
            t[2].close()
        size_mb = t[3] / (1024**2)
        print("WROTE %-26s %8.1f MB" % (t[4], size_mb))


if __name__ == "__main__":
    sys.exit(main())
