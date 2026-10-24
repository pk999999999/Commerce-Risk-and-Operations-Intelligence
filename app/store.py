"""Tenant-scoped PostgreSQL writes shared by API and stream processor."""
from __future__ import annotations

from decimal import Decimal
import hashlib
import os

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.auth import password_hash
from app.core import alerts_for_event, canonical_json, parse_time, validate_event


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def seed_demo() -> None:
    if os.getenv("APP_ENV") != "local":
        return
    password = os.environ["DEMO_ADMIN_PASSWORD"]
    with connect() as db:
        with db.cursor() as cur:
            for tenant_id, name, markets, email in [
                ("north-shop", "North Shop", ["US", "IN"], "admin@north.test"),
                ("harbor-store", "Harbor Store", ["GB", "US"], "admin@harbor.test"),
            ]:
                cur.execute("INSERT INTO tenants (id,name) VALUES (%s,%s) ON CONFLICT DO NOTHING", (tenant_id, name))
                for market in markets:
                    cur.execute("INSERT INTO markets (tenant_id,code) VALUES (%s,%s) ON CONFLICT DO NOTHING", (tenant_id, market))
                cur.execute("INSERT INTO tenant_config (tenant_id,version,fraud_amount,daily_event_quota,actor) VALUES (%s,1,5000,100000,'seed') ON CONFLICT DO NOTHING", (tenant_id,))
                cur.execute("SELECT id FROM users WHERE tenant_id=%s AND email=%s", (tenant_id, email))
                if not cur.fetchone():
                    cur.execute("INSERT INTO users (tenant_id,email,password_hash,role) VALUES (%s,%s,%s,'admin')", (tenant_id, email, password_hash(password)))


def process_event(event: dict) -> dict:
    event = validate_event(event)
    tenant, market, event_type = event["tenant_id"], event["market"], event["event_type"]
    payload = event["payload"]
    at = parse_time(event["occurred_at"])
    payload_hash = hashlib.sha256(canonical_json(event).encode()).hexdigest()
    with connect() as db:
        with db.cursor() as cur:
            cur.execute("SELECT 1 FROM markets WHERE tenant_id=%s AND code=%s", (tenant, market))
            if not cur.fetchone():
                raise ValueError("unregistered tenant or market")
            cur.execute("SELECT fraud_amount,daily_event_quota FROM tenant_config WHERE tenant_id=%s ORDER BY version DESC LIMIT 1", (tenant,))
            config = cur.fetchone()
            if not config:
                raise ValueError("missing tenant config")
            cur.execute("SELECT fraud_amount FROM market_rules WHERE tenant_id=%s AND market=%s ORDER BY version DESC LIMIT 1", (tenant, market))
            market_rule = cur.fetchone()
            threshold = market_rule["fraud_amount"] if market_rule else config["fraud_amount"]
            cur.execute("INSERT INTO processed_events (tenant_id,event_id,event_type,payload_hash,occurred_at) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING event_id", (tenant, event["event_id"], event_type, payload_hash, at))
            if not cur.fetchone():
                cur.execute("SELECT payload_hash FROM processed_events WHERE tenant_id=%s AND event_id=%s", (tenant, event["event_id"]))
                if cur.fetchone()["payload_hash"] != payload_hash:
                    raise ValueError("event_id reused with different payload")
                return {"inserted": False, "alerts": []}
            cur.execute("INSERT INTO usage_daily (tenant_id,day,event_count) VALUES (%s,%s,1) ON CONFLICT (tenant_id,day) DO UPDATE SET event_count=usage_daily.event_count+1 RETURNING event_count", (tenant, at.date()))
            if cur.fetchone()["event_count"] > config["daily_event_quota"]:
                raise ValueError("tenant daily event quota exceeded")
            if event_type == "order":
                cur.execute("""
                    INSERT INTO orders (tenant_id,order_id,market,amount,currency,email,order_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tenant_id,order_id) DO UPDATE SET
                      market=EXCLUDED.market,amount=EXCLUDED.amount,currency=EXCLUDED.currency,
                      email=EXCLUDED.email,order_at=EXCLUDED.order_at,updated_at=now()
                    WHERE orders.order_at IS NULL OR EXCLUDED.order_at >= orders.order_at
                """, (tenant, payload["order_id"], market, Decimal(str(payload["amount"])), payload["currency"], payload["email"], at))
            elif event_type == "payment":
                cur.execute("""
                    INSERT INTO orders (tenant_id,order_id,payment_status,payment_at)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (tenant_id,order_id) DO UPDATE SET
                      payment_status=EXCLUDED.payment_status,payment_at=EXCLUDED.payment_at,updated_at=now()
                    WHERE orders.payment_at IS NULL OR EXCLUDED.payment_at >= orders.payment_at
                """, (tenant, payload["order_id"], payload["status"], at))
            elif event_type == "shipment":
                cur.execute("""
                    INSERT INTO orders (tenant_id,order_id,shipment_status,shipment_at,eta_at)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (tenant_id,order_id) DO UPDATE SET
                      shipment_status=EXCLUDED.shipment_status,shipment_at=EXCLUDED.shipment_at,
                      eta_at=EXCLUDED.eta_at,updated_at=now()
                    WHERE orders.shipment_at IS NULL OR EXCLUDED.shipment_at >= orders.shipment_at
                """, (tenant, payload["order_id"], payload["status"], at, parse_time(payload["eta_at"])))
            alerts = alerts_for_event(event, threshold)
            new_alerts = []
            for alert in alerts:
                cur.execute("""
                    INSERT INTO alerts (tenant_id,alert_id,order_id,rule,severity,evidence)
                    VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING alert_id
                """, (tenant, alert["alert_id"], alert["order_id"], alert["rule"], alert["severity"], Jsonb(alert["evidence"])))
                if cur.fetchone():
                    new_alerts.append(alert)
            return {"inserted": True, "alerts": new_alerts}
