# Commerce Risk and Operations Intelligence

Multi-tenant local development platform for suspicious orders, shipment delays, and sales/fulfillment analytics. The [architecture decision record](docs/ADR-0001-architecture.md) describes the data flow, tenancy model, and tradeoffs.

## Status

The repository contains the local end-to-end slice and daily reconciliation code. It has not been run under Docker in the current development environment because Docker is unavailable. Pure Python contract and authentication tests pass; live integration tests are provided but need a running stack. Do not treat the Compose configuration as a production deployment.

## Local setup

Requirements: Docker Engine with Compose v2, at least 4 CPU cores and 8–12 GiB free RAM. Copy `.env.example` to `.env` and change the secrets. Run `docker compose up --build`. Open `http://localhost:8000`. Seeded organizations are `north-shop` (US, IN) and `harbor-store` (GB, US). The demo account is `admin@north.test` and the password in `DEMO_ADMIN_PASSWORD`. The synthetic generator sends a small finite dataset by default.

Data volumes persist across restarts. `docker compose down` stops services; `docker compose down -v` removes all local database and broker data. Never use the demo credentials on a network-exposed deployment. See [operations](docs/OPERATIONS.md) for health checks, replay, onboarding, Airflow, backup, and restore.

## Planned operating commands

`docker compose logs -f processor` follows streaming work. `docker compose run --rm generator` emits another deterministic demo run. `docker compose --profile loadtest run --rm load-generator` is the explicit larger-volume path. Airflow is intended at `http://localhost:8080`; run the `daily_reconciliation` DAG after events land.

## Implemented versus planned

| Capability | State |
|---|---|
| Versioned event contract, two-tenant generator, duplicates, late and out-of-order events | Implemented; unit tested |
| Kafka, Spark stream, raw/dead-letter/curated Parquet | Implemented; Docker runtime unverified |
| PostgreSQL control plane, deterministic rules and alerts, API auth and tenant-scoped queries | Implemented; cross-tenant live tests need Docker |
| ClickHouse order facts, Elasticsearch search, investigator UI | Implemented; Docker runtime unverified |
| Airflow daily Spark replay and ClickHouse daily metrics | Implemented; Docker runtime unverified |
| Tenant onboarding CLI, roles, API keys, quotas, audit, configuration history | Implemented; Docker runtime unverified |
| Production TLS, least-privilege roles, RLS, full deletion, observability, cloud adapters, Storm, Flume | Planned |

The [load-test report](docs/LOAD_TEST.md) records the unmeasured status and a reproducible measurement plan. No scale target is claimed. CI runs unit checks, a Python dependency audit, and a Compose smoke test on pushes and pull requests.

## Software licenses

Kafka, Spark, Airflow: Apache 2.0. PostgreSQL: PostgreSQL License. ClickHouse: Apache 2.0. Elasticsearch: Elastic License 2.0 / SSPL / AGPL options vary by version and distribution; this local build uses the official self-managed distribution's free features. FastAPI: MIT. Check actual image and package license terms before redistribution.
