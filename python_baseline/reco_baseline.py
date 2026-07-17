#!/usr/bin/env python3

import sys
import time
from collections import defaultdict

# csv column indices
IDX_EVENT_TYPE = 1
IDX_PRODUCT_ID = 2
IDX_USER_SESSION = 8
MIN_FIELDS = 9
MAX_SESSION_SIZE = 50

def peak_rss_mb():
    try:
        import resource
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024.0
    except Exception:
        return float("nan")

def main():
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: python reco_baseline.py <in> <out> [events] [topN]\n")
        return 2

    in_path = sys.argv[1]
    out_path = sys.argv[2]
    
    raw_events = sys.argv[3] if len(sys.argv) > 3 else "cart,purchase"
    event_types = {t.strip().lower() for t in raw_events.split(",") if t.strip()}
    
    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 10

    t0 = time.time()

    # stage 1a: read file and group distinct products by session
    sessions = defaultdict(set)
    rows = 0
    kept = 0
    
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
                kept += 1
            except ValueError:
                continue
                
    t_parse = time.time()

    # stage 1b: build stripes (anchor -> {other: count})
    stripes = defaultdict(lambda: defaultdict(int))
    
    for prods in sessions.values():
        unique_prods = sorted(list(prods))
        n = len(unique_prods)
        
        # skip bot sessions or 1-item sessions
        if n < 2 or n > MAX_SESSION_SIZE:
            continue
            
        for anchor in unique_prods:
            for other in unique_prods:
                if anchor != other:
                    stripes[anchor][other] += 1

    t_stripes = time.time()

    # stage 2: sort, get top N for each anchor, and write output
    with open(out_path, "w", encoding="utf-8") as out:
        for anchor in sorted(stripes.keys()):
            # sort by count desc, then by product id asc
            recos = sorted(stripes[anchor].items(), key=lambda x: (-x[1], x[0]))[:top_n]
            recos_str = ",".join(f"{other}:{count}" for other, count in recos)
            out.write(f"{anchor}\t{recos_str}\n")
            
    t_end = time.time()

    sys.stderr.write(
        f"[baseline] rows={rows} kept_events={kept} sessions={len(sessions)} anchors={len(stripes)}\n"
    )
    sys.stderr.write(
        f"[baseline] parse={t_parse - t0:.1f}s stripes={t_stripes - t_parse:.1f}s rank+write={t_end - t_stripes:.1f}s TOTAL={t_end - t0:.1f}s peakRSS={peak_rss_mb():.0f}MB\n"
    )
    
    return 0

if __name__ == "__main__":
    sys.exit(main())