CREATE DATABASE IF NOT EXISTS commerce;
CREATE TABLE IF NOT EXISTS commerce.order_facts (
  tenant_id String,
  order_id String,
  market LowCardinality(String),
  amount Decimal(18, 2),
  currency LowCardinality(String),
  order_at DateTime64(3, 'UTC'),
  version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY cityHash64(tenant_id) % 16
ORDER BY (tenant_id, order_id);

CREATE TABLE IF NOT EXISTS commerce.daily_metrics (
  tenant_id String,
  day Date,
  market LowCardinality(String),
  currency LowCardinality(String),
  order_count UInt64,
  gross_amount Decimal(18, 2),
  version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, day, market, currency);
