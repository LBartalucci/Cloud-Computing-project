#!/usr/bin/env python3

import sys
from pyspark import SparkConf, SparkContext

# csv column indices
IDX_EVENT = 1
IDX_PROD = 2
IDX_SESSION = 8
MIN_COLS = 9
MAX_SESSION = 50

def main():
    if len(sys.argv) < 3:
        print("Usage: spark-submit reco_spark.py <in> <out> [events] [topN] [parts]")
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

    conf = SparkConf().setAppName("reco-spark-stripes")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # broadcast valid events to all workers to save memory
    bc_events = sc.broadcast(valid_events)

    def parse_row(line):
        # skip empty lines or headers
        if not line or line.startswith("event_time"):
            return None

        cols = line.split(",")
        # skip malformed rows
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

    def get_stripes(products):
        # dedupe items in the session
        unique_prods = sorted(list(set(products)))
        n = len(unique_prods)

        # skip bot sessions or 1-item sessions
        if n < 2 or n > MAX_SESSION:
            return []

        # generate a stripe for each anchor product
        stripes = []
        for anchor in unique_prods:
            stripe = {}
            for other in unique_prods:
                if anchor != other:
                    stripe[other] = 1
            stripes.append((anchor, stripe))
        return stripes

    def merge_stripes(stripe1, stripe2):
        # merge two dictionaries

        for prod, count in stripe2.items():
            stripe1[prod] = stripe1.get(prod, 0) + count
        return stripe1

    def extract_top_n(stripe):
        # sort by frequency desc, then by product ID asc
        return sorted(stripe.items(), key=lambda x: (-x[1], x[0]))[:top_n]

    def format_output(kv):
        # format like the hadoop output: anchor \t other1:count,other2:count...
        anchor, recos = kv
        reco_str = ",".join(f"{other}:{count}" for other, count in recos)
        return f"{anchor}\t{reco_str}"

    # read input
    raw_data = sc.textFile(in_path)

    # parse to (session, product)
    session_items = raw_data.map(parse_row).filter(lambda x: x is not None)

    # group products by session
    if num_parts:
        grouped_sessions = session_items.groupByKey(num_parts)
    else:
        grouped_sessions = session_items.groupByKey()

    # stage 1 & 2 combined
    # flatMap (anchor, stripe_dict)
    # reduceByKey automatically does map combine 
    aggregated_stripes = grouped_sessions.flatMap(lambda kv: get_stripes(kv[1])) \
                                         .reduceByKey(merge_stripes)

    # extract top N for each anchor
    top_recos = aggregated_stripes.mapValues(extract_top_n)

    # format and save
    top_recos.map(format_output).saveAsTextFile(out_path)

    sc.stop()

if __name__ == "__main__":
    main()