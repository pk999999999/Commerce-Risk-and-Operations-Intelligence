CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  retention_days INTEGER NOT NULL DEFAULT 90 CHECK (retention_days BETWEEN 1 AND 3650),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS markets (
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  code TEXT NOT NULL,
  PRIMARY KEY (tenant_id, code)
);
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  email TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('admin','investigator','viewer')),
  active BOOLEAN NOT NULL DEFAULT true,
  UNIQUE (tenant_id, email)
);
CREATE TABLE IF NOT EXISTS api_keys (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  label TEXT NOT NULL,
  key_hash TEXT NOT NULL UNIQUE,
  role TEXT NOT NULL CHECK (role IN ('admin','investigator','viewer')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS processed_events (
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  event_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, event_id)
);
CREATE TABLE IF NOT EXISTS orders (
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  order_id TEXT NOT NULL,
  market TEXT,
  amount NUMERIC(18,2),
  currency TEXT,
  email TEXT,
  order_at TIMESTAMPTZ,
  payment_status TEXT,
  payment_at TIMESTAMPTZ,
  shipment_status TEXT,
  shipment_at TIMESTAMPTZ,
  eta_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, order_id)
);
CREATE INDEX IF NOT EXISTS orders_tenant_time_idx ON orders (tenant_id, order_at DESC);
CREATE TABLE IF NOT EXISTS alerts (
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  alert_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  rule TEXT NOT NULL,
  severity TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','investigating','closed')),
  evidence JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, alert_id)
);
CREATE INDEX IF NOT EXISTS alerts_tenant_created_idx ON alerts (tenant_id, created_at DESC);
CREATE TABLE IF NOT EXISTS tenant_config (
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  version INTEGER NOT NULL,
  fraud_amount NUMERIC(18,2) NOT NULL CHECK (fraud_amount > 0),
  daily_event_quota INTEGER NOT NULL CHECK (daily_event_quota > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor TEXT NOT NULL,
  PRIMARY KEY (tenant_id, version)
);
CREATE TABLE IF NOT EXISTS audit_logs (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  subject TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS usage_daily (
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  day DATE NOT NULL,
  event_count BIGINT NOT NULL DEFAULT 0,
  api_count BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, day)
);
