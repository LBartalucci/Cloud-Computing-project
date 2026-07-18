#!/usr/bin/env python3
# pyspark port of our ecommerce recommender
# follows hadoop implementation with pairs (session pairs -> top N recommendations)

import sys
from operator import add
from pyspark import SparkConf, SparkContext

# csv column indices
IDX_EVENT = 1
IDX_PROD = 2
IDX_SESSION = 8
MIN_COLS = 9
MAX_SESSION = 50

def main():
    if len(sys.argv) < 3:
        print("usage: spark-submit reco_spark.py <in> <out> [events] [topN] [parts]")
        sys.exit(2)

    in_path = sys.argv[1]
    out_path = sys.argv[2]

    # parse events
    raw_events = sys.argv[3] if len(sys.argv) > 3 else "cart,purchase"
    valid_events = {e.strip().lower() for e in raw_events.split(",") if e.strip()}

    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 10

    # handle partitions
    parts_arg = sys.argv[5] if len(sys.argv) > 5 else None
    num_parts = int(parts_arg) if parts_arg and parts_arg != "-" else None

    conf = SparkConf().setAppName("reco-spark-job")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # broadcast valid events to save memory
    bc_events = sc.broadcast(valid_events)

    def parse_row(line):
        if not line or line.startswith("event_time"):
            return None

        cols = line.split(",")
        if len(cols) < MIN_COLS:
            return None

        event = cols[IDX_EVENT].strip().lower()
        if event not in bc_events.value:
            return None

        session = cols[IDX_SESSION].strip()
        prod = cols[IDX_PROD].strip()

        if not session or not prod:
            return None

        try:
            return (session, int(prod))
        except ValueError:
            return None

    def get_pairs(products_iterable):
        # deduplicate inside session to prevent OOM and match baseline logic
        unique_prods = sorted(list(set(products_iterable)))
        n = len(unique_prods)

        # filter out bots and single-item sessions
        if n < 2 or n > MAX_SESSION:
            return []

        # generate unique pairings (a < b)
        pairs = []
        for i in range(n):
            for j in range(i + 1, n):
                pairs.append(((unique_prods[i], unique_prods[j]), 1))
        return pairs

    # --- STAGE 1: session pairs ---
    raw_data = sc.textFile(in_path)
    session_items = raw_data.map(parse_row).filter(lambda x: x is not None)

    if num_parts:
        grouped_sessions = session_items.groupByKey(num_parts)
    else:
        grouped_sessions = session_items.groupByKey()

    pair_freq = grouped_sessions.flatMap(lambda kv: get_pairs(kv[1])).reduceByKey(add)

    # --- STAGE 2: top N recommendations ---
    # map to directed associations: (anchor, (neighbor, count))
    directed_pairs = pair_freq.flatMap(
        lambda kv: [(kv[0][0], (kv[0][1], kv[1])), (kv[0][1], (kv[0][0], kv[1]))]
    )

    # aggregate locally on workers before shuffling to optimize network I/O
    def merge_value(current_top, new_item):
        current_top.append(new_item)
        current_top.sort(key=lambda x: (-x[1], x[0]))
        return current_top[:top_n]

    def merge_combiners(top_list1, top_list2):
        combined = top_list1 + top_list2
        combined.sort(key=lambda x: (-x[1], x[0]))
        return combined[:top_n]

    zero_value = []

    if num_parts:
        top_recos = directed_pairs.aggregateByKey(zero_value, merge_value, merge_combiners, num_partitions=num_parts)
    else:
        top_recos = directed_pairs.aggregateByKey(zero_value, merge_value, merge_combiners)

    # format output as: anchor \t other1:count,other2:count...
    def format_output(kv):
        anchor, recos = kv
        reco_str = ",".join(f"{other}:{count}" for other, count in recos)
        return f"{anchor}\t{reco_str}"

    top_recos.map(format_output).saveAsTextFile(out_path)
    sc.stop()

if __name__ == "__main__":
    main()