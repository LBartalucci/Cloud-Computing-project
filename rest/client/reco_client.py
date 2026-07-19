#!/usr/bin/env python3
"""Command-line client for the recommender REST service.

Submits a job, polls until it finishes, prints the timing/statistics, and
optionally fetches the recommendations for a specific product.

Examples:
  python reco_client.py --server http://10.1.1.156:5000 \
      --engine spark --dataset dev --product 1004856

  python reco_client.py --engine hadoop --dataset main --reducers 4 --topn 20
"""
import argparse
import sys
import time

import requests


def main():
    ap = argparse.ArgumentParser(description="Recommender REST client")
    ap.add_argument("--server", default="http://localhost:5000")
    ap.add_argument("--engine", choices=["hadoop", "spark", "baseline"], default="hadoop")
    ap.add_argument("--dataset", choices=["dev", "main"], default="dev")
    ap.add_argument("--event-types", default="cart,purchase")
    ap.add_argument("--topn", type=int, default=10)
    ap.add_argument("--reducers", type=int, default=2)
    ap.add_argument("--product", type=int, help="fetch recommendations for this product id")
    ap.add_argument("--poll", type=float, default=3.0, help="poll interval seconds")
    ap.add_argument("--timeout", type=float, default=3600, help="max wait seconds")
    args = ap.parse_args()

    payload = {
        "engine": args.engine, "dataset": args.dataset,
        "event_types": args.event_types, "topn": args.topn, "reducers": args.reducers,
    }
    r = requests.post(args.server + "/jobs", json=payload, timeout=30)
    r.raise_for_status()
    job_id = r.json()["job_id"]
    print("submitted job %s (%s on %s)" % (job_id, args.engine, args.dataset))

    deadline = time.time() + args.timeout
    state = "PENDING"
    while time.time() < deadline:
        time.sleep(args.poll)
        j = requests.get("%s/jobs/%s" % (args.server, job_id), timeout=30).json()
        if j["state"] != state:
            state = j["state"]
            print("  state -> %s" % state)
        if state in ("SUCCEEDED", "FAILED"):
            break

    j = requests.get("%s/jobs/%s" % (args.server, job_id), timeout=30).json()
    print("final state: %s  exit_code=%s  duration=%ss" %
          (j["state"], j.get("exit_code"), j.get("duration_s")))
    print("stats:", j.get("stats"))
    if j["state"] != "SUCCEEDED":
        log = requests.get("%s/jobs/%s/log" % (args.server, job_id), timeout=30).text
        print("---- log tail ----")
        print("\n".join(log.splitlines()[-20:]))
        return 1

    if args.product is not None:
        u = "%s/jobs/%s/recommendations/%d?limit=%d" % (
            args.server, job_id, args.product, args.topn)
        rr = requests.get(u, timeout=60)
        data = rr.json()
        print("recommendations for product %d:" % args.product)
        for item in data.get("recommendations", []):
            print("  %d  (score %d)" % (item["product"], item["score"]))
        if not data.get("recommendations"):
            print("  (none / product not present)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
