# Tiny REST gateway image: Python + Flask + paramiko. It does NOT bundle Hadoop;
# it submits jobs to the cluster over SSH (see cluster.py).
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py cluster.py openapi.yaml ./

EXPOSE 5000

# Single worker (in-memory job registry) with a thread pool so polling stays
# responsive while jobs run.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "8", \
     "--timeout", "0", "app:app"]
