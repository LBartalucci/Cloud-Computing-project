# Mining Product Co-occurrence for Recommendations: Hadoop vs. Spark

**Cloud Computing project, A.Y. 2025/26** — E-commerce session co-occurrence → product recommendations.

---

## 1. Problem and application scenario

An online multi-category store wants *"customers who interacted with X also interacted with Y"*
recommendations. From anonymized behavioural logs (view / cart / purchase events grouped into
user sessions) we build a **product co-occurrence model** and, for every product, rank the
strongest co-occurring products.

The analytics result is exposed through a **containerized REST API**: a client submits a job
(Hadoop, Spark or baseline), polls for completion, and retrieves the top-N co-occurring products
for any anchor product id.

The same workflow is implemented three ways and compared experimentally:

| Engine | Technology | Role |
|--------|------------|------|
| **Hadoop** | Java MapReduce (2 jobs in cascade) | Primary distributed implementation |
| **Spark** | PySpark on YARN | Same logic, alternative framework |
| **Baseline** | Single-process Python | Non-parallel reference ("does not scale") |

---

## 2. Dataset

**REES46 "eCommerce behavior data from a multi-category store"**, month `2019-Oct`
(≈5.3 GB, 42.4 M rows). Source: Kaggle. The raw file is **not** committed (per guidelines).

CSV schema (header):

```
event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
```

**Preprocessing.** We use temporal prefixes of the month as workload slices: 150 MB, 500 MB,
1 GB and 1.5 GB (`scripts/make_slices.py`). Because sessions are interleaved by `event_time`
across the file, a prefix is a valid subset. Slices are loaded into HDFS under
`/user/hadoop/ecommerce/`.

**Event filter.** By default we keep `cart` and `purchase` events (`cc.event.types`), which
best express purchase intent for recommendations. The filter is configurable via job parameters.

---

## 3. Analytics workflow — two MapReduce jobs in cascade

The workflow is split into **two non-iterative jobs with distinct purposes**; Job 2 consumes
Job 1's output. This is a genuine cascade, not one task artificially split in two.

```
  REES46 CSV (HDFS)
        │
        ▼
  ┌─────────────────────────────────────┐
  │ Job 1 — SessionPairs                │
  │  session → distinct products        │
  │  → unordered co-occurring pairs     │
  └─────────────────┬───────────────────┘
                    │  pairs (a,b) → 1
                    ▼
  ┌─────────────────────────────────────┐
  │ Job 2 — Recommend                   │
  │  aggregate counts per anchor product│
  │  → top-N co-occurring products      │
  └─────────────────┬───────────────────┘
                    │
                    ▼
        anchor \t other1:c1,other2:c2,...
```

### Job 1 — Session co-occurrence generation (`SessionPairs`)

*Purpose: reconstruct session baskets and enumerate co-occurring product pairs.*

- **Map:** parse each CSV row; keep rows whose `event_type` is in the configured set;
  emit `(user_session → product_id)`. `setup()` reads `cc.event.types` from the job
  configuration.
- **Reduce:** per session, dedupe products in a `HashSet`; skip sessions larger than
  `cc.max.session.size` (default 50) to avoid O(n²) pair explosion from bot traffic;
  emit `(ProductPair(a,b) → 1)` for every unordered pair with `a < b`.
- **MapReduce features used:** custom `WritableComparable` (`ProductPairWritable` with
  canonical ordering), `setup()` for configuration.

### Job 2 — Aggregation & top-N ranking (`Recommend`)

*Purpose: turn pair instances into co-occurrence scores and rank top-N per product.*

- **Map:** read Job 1 output (`a\tb\t1`); emit **both directions**
  `(anchor → ProductCount(other, count))` so either product of a pair can be recommended.
- **Combiner:** locally sum counts per `(anchor, other)` to reduce shuffle volume.
- **Partitioner:** `AnchorPartitioner` routes all records for the same anchor product to
  the same reducer (partition on product id, not generic hashCode).
- **Reduce:** `setup()` reads `cc.topn`; globally aggregate counts; keep top-N with a
  bounded min-heap; emit `anchor \t other1:count1,other2:count2,...` sorted by count
  descending, then product id ascending.
- **MapReduce features used:** combiner, custom partitioner, multiple reducers, `setup()`,
  second custom `Writable` (`ProductCountWritable`).

### Driver

`RecoDriver` (`Tool` / `ToolRunner`) chains Job 1 → Job 2. Job 1 writes to `<output>_stage1`;
Job 2 reads that path and writes the final recommendations to `<output>`. The intermediate
directory is deleted unless `cc.keep.intermediate=true`.

Launch:

```bash
hadoop jar ecommerce-reco.jar it.unipi.hadoop.RecoDriver \
  -Dcc.event.types=cart,purchase -Dcc.topn=10 \
  <hdfs_input> <hdfs_output> [numReducers]
```

### Spark implementation (`reco_spark.py`)

Mirrors both stages with the RDD API:

1. `map` / `filter` → `groupByKey` (by session) → `flatMap` (unordered pairs) → `reduceByKey`
2. `flatMap` (both directions) → `groupByKey` (by anchor) → top-N per anchor

Runs on YARN in client mode (2 executors × 512 MB). Output format matches Hadoop exactly.

### Python baseline (`reco_baseline.py`)

Single-process implementation with Python `defaultdict` / `set`. Reads a local CSV file,
holds the full co-occurrence map in RAM, writes the same output format. Instrumented with
wall-clock time and peak RSS for the experimental comparison.

### Correctness

All three implementations use identical ranking logic (count desc, product id asc on ties).
Verified on the 150 MB dev slice with `scripts/verify_parity.sh`: **1503 anchor products,
zero diff** between Hadoop, Spark and the baseline.

---

## 4. Repository structure

```
hadoop/          Maven project: MapReduce jobs (Java)
spark/           PySpark implementation of the same workflow
python_baseline/ Non-parallel Python implementation (for comparison)
rest/            Dockerized Flask REST server + client (async job submission)
scripts/         Dataset preprocessing, HDFS upload, run/benchmark helpers
ops/             Cluster operations helpers (env, inventory, VM sync)
report/          This report and curated benchmark results
data/            Local dataset slices (raw CSV is git-ignored)
```

### Component reference

**`hadoop/`** — MapReduce cascade (Java, Maven 3.6, Hadoop 3.1.3 provided scope)

| File | Role |
|------|------|
| `SessionPairs.java` | Job 1 mapper/reducer |
| `Recommend.java` | Job 2 mapper, combiner, reducer |
| `ProductPairWritable.java` | Custom key for unordered product pairs |
| `ProductCountWritable.java` | Custom value (product id, count) |
| `AnchorPartitioner.java` | Partition by anchor product id |
| `RecoDriver.java` | Cascade driver |
| `pom.xml` | Builds `target/ecommerce-reco.jar` |

**`spark/`** — `reco_spark.py`: PySpark RDD pipeline on YARN.

**`python_baseline/`** — `reco_baseline.py`: non-parallel reference with timing/RSS stats.

**`rest/`** — Containerized REST gateway (intermediate layer between client and cluster)

| File | Role |
|------|------|
| `server/app.py` | Flask API: async job submission and status polling |
| `server/cluster.py` | SSH gateway to submit jobs and read HDFS results |
| `server/Dockerfile` | Python 3.11 + Flask + paramiko (no Hadoop in image) |
| `server/openapi.yaml` | OpenAPI 3 specification |
| `client/reco_client.py` | CLI client: submit → poll → recommendations |
| `docker-compose.yml` | Host-network deployment on the namenode |

**`scripts/`** — Run helpers, dataset tools, benchmark harness, parity verification.

**`ops/`** — `vm.py` / `sync.py` for non-interactive VM management from a developer PC;
`hadoop-env.sh` for sourcing Hadoop/Spark paths in non-interactive shells.

---

## 5. System architecture & deployment

### Cluster topology

Two-node fully-distributed cluster (per course "Hadoop 3 Installation Notes"):

| Host | IP | Roles |
|------|----|-------|
| `hadoop-namenode` | 10.1.1.156 | NameNode, ResourceManager, DataNode, NodeManager, Spark client, **REST server (Docker)** |
| `hadoop-datanode-2` | 10.1.1.150 | DataNode, NodeManager |

Software: Hadoop 3.1.3, Spark 3.4.4 (without-hadoop build, `SPARK_DIST_CLASSPATH` from
Hadoop), Java 8, Ubuntu 22.04, Docker 29.x on the namenode.

Hadoop/Spark jobs run as the `hadoop` user. Non-interactive sessions must
`source /home/hadoop/cc_ops/hadoop-env.sh` (the `~/.bashrc` exports sit after the
non-interactive early-return).

### REST architecture

The REST service is the **required intermediate layer** between the client and the distributed
backend. Design choices:

- **Async model:** `POST /jobs` returns immediately with a `job_id`; a background worker
  drives the cluster; the client polls `GET /jobs/{id}`.
- **Thin container:** the Docker image contains only Python + Flask + paramiko. It submits
  jobs over SSH to localhost (the namenode) and reads results from HDFS — it does not embed
  Hadoop or Spark.
- **Three engines:** the same REST API accepts `engine: hadoop | spark | baseline` and
  `dataset: dev | main`, mapping to pre-loaded HDFS paths and local CSV files.

**API endpoints:**

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness probe |
| POST | `/jobs` | Submit job → `202 {job_id}` |
| GET | `/jobs/{id}` | Status, timing, parsed statistics |
| GET | `/jobs/{id}/log` | Captured job log |
| GET | `/jobs/{id}/recommendations/{product}` | Top-N co-occurring products |
| GET | `/openapi.yaml` | OpenAPI 3 spec |

Validated end-to-end: Windows client → REST on `.156` → SSH job submission → HDFS results →
recommendation query, for all three engines with identical output for product `1004856`.

---
