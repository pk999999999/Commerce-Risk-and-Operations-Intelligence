# Operating the local system

## Start and verify

1. Install Docker Engine and Compose v2, then copy `.env.example` to `.env`. Replace all example secrets. `TOKEN_SECRET` must be at least 32 characters.
2. Run `docker compose config` to validate interpolation, then `docker compose up --build`.
3. Wait for `api`, `postgres`, `kafka`, `clickhouse`, and `elasticsearch` health checks. The one-shot `generator` should exit 0 after publishing a small demo batch. Open `http://127.0.0.1:8000`.
4. Sign in to `north-shop` as `admin@north.test` or `harbor-store` as `admin@harbor.test` with `DEMO_ADMIN_PASSWORD`. Compare their alerts and orders.
5. Get Airflow's local standalone admin password with `docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`, then open `http://127.0.0.1:8080`. Trigger `daily_reconciliation` manually or wait for its daily schedule. Check `/api/analytics/daily` with a bearer token.
6. Run pure Python tests with `python -m unittest discover -s tests -v`. To run live tenant checks, set `COMMERCE_INTEGRATION_URL=http://127.0.0.1:8000` and `DEMO_ADMIN_PASSWORD` in the test shell, then run the same command.

The default generator is intentionally small. `docker compose run --rm generator` repeats its daily deterministic IDs and tests replay. `docker compose --profile loadtest run --rm load-generator` publishes 100,000 orders plus related events only when explicitly requested. Onboarding an additional organization uses `docker compose run --rm -it api python -m app.onboard new-tenant 'New Tenant' admin@example.test --market US`; the command prompts for its password.

## Dependency and license notes

The build pulls free, self-hosted images and Python packages but still needs internet for image and Maven package downloads. The Spark Kafka connector is fetched by `spark-submit --packages` on first startup. Cache or mirror dependencies for offline environments. Kafka, Spark, Airflow, and ClickHouse use Apache 2.0; PostgreSQL uses the PostgreSQL License; FastAPI uses MIT. Elasticsearch's official self-managed distribution includes free features under Elastic's published licensing terms. Review the exact pinned distribution before redistribution.

The Compose stack is a development deployment. Local Elasticsearch security is disabled, Kafka uses plaintext, database credentials are shared by service containers, and Airflow standalone is a convenience deployment. Bind ports to loopback only. Do not expose this stack to the public internet.

## Failure and recovery

| Symptom | Check | Recovery |
|---|---|---|
| No new alerts | `docker compose logs processor kafka-init generator` | Confirm topic creation and Spark checkpoint progress; restart processor after fixing the dependency. |
| Invalid records | Inspect Parquet under `lake/dead_letter` through a Spark session | Fix the producer contract, then republish corrected events with new event IDs. |
| Search misses an alert | Compare `/api/alerts` with `/api/alerts/search` | Trigger `daily_reconciliation` to rebuild the index from PostgreSQL. |
| Daily metrics stale | Inspect Airflow DAG run logs and ClickHouse health | Rerun `daily_reconciliation`; ReplacingMergeTree keys make publishing repeatable. |
| Kafka offset loss | Check broker volume and Spark checkpoints | Restore both from a matched backup, or replay from earliest while retaining PostgreSQL business keys. |

PostgreSQL is authoritative for orders, alerts, users, and audit. The raw Parquet lake is the replay source for reconciliation. A stream microbatch can finish PostgreSQL and fail before ClickHouse; rerunning the batch job repairs the derived store. The search index is rebuilt from PostgreSQL. Do not delete checkpoints to troubleshoot a live cluster without a replay plan.

## Backup and restore

For local development, use `docker compose exec -T postgres pg_dump -U commerce -Fc commerce > commerce.dump` from a shell with a secure destination. Back up the `lake`, `checkpoints`, Kafka, and ClickHouse volumes separately with a coordinated snapshot; PostgreSQL alone is insufficient for a complete recovery. Restore PostgreSQL with `pg_restore` into an empty database, restore matching lake and Kafka snapshots, then run reconciliation. Test restore in an isolated Compose project before using backups operationally. Avoid committing dumps or credentials.

## Monitoring and security work still required

Current health checks cover process readiness; structured Spark logs and audit rows provide basic diagnostics. Before production, add metrics and trace export, Kafka lag alerts, index reconciliation alerts, restore drills, TLS, Kafka ACLs, least-privilege database roles, RLS policies, Elasticsearch authentication, a secret manager, image and dependency scanning, container read-only filesystems where possible, and a multi-node deployment plan. The fixed Postgres-backed 120 requests/minute tenant limit is local protection; distributed production rate limiting and per-plan limits need design.

Retention is represented in tenant configuration, but automated deletion across PostgreSQL, lake, Kafka, ClickHouse, and Elasticsearch is not implemented. A tenant deletion request must be handled as an operational project until a verifiable workflow exists. Never claim deletion has completed by removing only one store.

## Optional cloud and legacy integrations

The `LakePaths` interface currently accepts only local storage. A GCS implementation would add a Spark Hadoop GCS connector and bucket IAM. A BigQuery publisher would read reconciled facts and require an explicit `ENABLE_BIGQUERY` flag, service identity, dataset configuration, dry-run cost estimate, and approval before any billable resource is created. Dataproc submission would package the existing Spark jobs and use a preapproved cluster or a separately approved provisioning step. No cloud resources are created by this repository.

Storm is reserved for a measured need for a separate subsecond rules path; it must not mirror the Spark pipeline. Flume is reserved for a legacy application log source that cannot emit the event contract. Neither is needed for the working local path.
