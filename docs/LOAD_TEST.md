# Load-test report

Status: **not run**. No throughput, end-to-end latency, failure behavior, or scale target is claimed.

## Available host

Development host observed on 2026-10-24: Windows, 18 logical CPUs, 16,641,941,504 bytes of RAM. Docker was not installed in the execution environment, so the Compose stack and load-test profile could not run. Disk type, network bandwidth, and effective Docker resource limits were not measured.

## Reproducible test plan

1. Start the default stack and verify tenant isolation and daily reconciliation before load testing.
2. Record CPU model, usable RAM, disk, Docker limits, image digests, and current Git commit.
3. Start collectors for Kafka consumer lag, Spark microbatch duration and input rate, PostgreSQL transactions and disk, ClickHouse insert latency, Elasticsearch indexing, API request latency, and system CPU/memory.
4. Run `docker compose --profile loadtest run --rm load-generator` only after confirming free disk space. It emits 100,000 orders and related payment and shipment events. Record start/end timestamps and total delivery acknowledgments.
5. Measure p50/p95/p99 elapsed time from Kafka publish to alert appearing in `/api/alerts`, plus daily reconciliation runtime. Inject one processor restart mid-run, then verify final order and alert counts by tenant, event deduplication, and search/analytics convergence.
6. Repeat on a clean stack and record bottlenecks. Do not infer scale from generator publish rate alone.

## Results

| Metric | Result |
|---|---|
| Hardware used for an actual run | Not measured |
| Sustained event throughput | Not measured |
| Alert end-to-end latency | Not measured |
| Recovery after processor failure | Not measured |
| Bottleneck | Not measured |
