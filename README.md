# Commerce Risk and Operations Intelligence

Multi-tenant local development platform for suspicious orders, shipment delays, and sales/fulfillment analytics. The [architecture decision record](docs/ADR-0001-architecture.md) describes the data flow, tenancy model, and tradeoffs.

## Status

This repository is under phased construction. The first milestone is a local end-to-end slice. Do not treat the Compose configuration as a production deployment. The implementation and verification status below are updated with each milestone.

## Local setup

Requirements: Docker Engine with Compose v2, at least 4 CPU cores and 8–12 GiB free RAM. Copy `.env.example` to `.env` and change the secrets. Run `docker compose up --build`. Open `http://localhost:8000`. Seeded organizations are `north-shop` (US, IN) and `harbor-store` (GB, US). The demo account is `admin@north.test` and the password in `DEMO_ADMIN_PASSWORD`. The synthetic generator sends a small finite dataset by default.

Data volumes persist across restarts. `docker compose down` stops services; `docker compose down -v` removes all local database and broker data. Never use the demo credentials on a network-exposed deployment.

## Planned operating commands

`docker compose logs -f processor` follows streaming work. `docker compose run --rm generator` emits another deterministic demo run. `docker compose --profile loadtest run --rm load-generator` is the explicit larger-volume path. Airflow is intended at `http://localhost:8080`; run the `daily_reconciliation` DAG after events land.

## Implemented versus planned

The contract and architecture are documented. Runtime components and their exact verification state will be listed here as they are added. Production hardening, cloud adapters, Storm, and Flume are planned, not included in the local milestone.

## Software licenses

Kafka, Spark, Airflow: Apache 2.0. PostgreSQL: PostgreSQL License. ClickHouse: Apache 2.0. Elasticsearch: Elastic License 2.0 / SSPL / AGPL options vary by version and distribution; this local build uses the official self-managed distribution's free features. FastAPI: MIT. Check actual image and package license terms before redistribution.
