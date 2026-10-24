"""Small, deterministic two-tenant event stream. Larger runs need --count."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import random
import time
from uuid import uuid5, NAMESPACE_URL

from confluent_kafka import Producer

from app.core import canonical_json


def make_event(tenant: str, market: str, kind: str, key: str, payload: dict, at: datetime) -> dict:
    return {
        "schema_version": 1,
        "event_id": str(uuid5(NAMESPACE_URL, f"commerce-demo:{tenant}:{kind}:{key}")),
        "tenant_id": tenant,
        "market": market,
        "event_type": kind,
        "occurred_at": at.isoformat(),
        "payload": payload,
    }


def generate(count: int = 24, seed: int = 17) -> list[dict]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events: list[dict] = []
    for i in range(count):
        tenant, market, currency = ("north-shop", "US", "USD") if i % 2 == 0 else ("harbor-store", "GB", "GBP")
        if i % 6 == 0:
            market = "IN" if tenant == "north-shop" else "US"
            currency = "INR" if market == "IN" else "USD"
        order_id = f"demo-{i:06d}"
        amount = 7500 if i % 11 == 0 else rng.randint(20, 350)
        at = now - timedelta(minutes=i)
        order = make_event(tenant, market, "order", order_id, {"order_id": order_id, "amount": amount, "currency": currency, "email": f"buyer{i}@example.test"}, at)
        payment = make_event(tenant, market, "payment", order_id, {"order_id": order_id, "status": "captured", "amount": amount}, at + timedelta(seconds=10))
        shipment = make_event(tenant, market, "shipment", order_id, {"order_id": order_id, "status": "delayed" if i % 7 == 0 else "in_transit", "eta_at": (at + timedelta(days=2)).isoformat()}, at + timedelta(seconds=20))
        events.extend([order, payment, shipment])
    events.append(make_event("north-shop", "US", "inventory", "sku-1", {"sku": "sku-1", "available": 7}, now))
    events.append(make_event("harbor-store", "GB", "application", "app-1", {"level": "INFO", "message": "checkout healthy"}, now))
    events.append(events[0].copy())  # duplicate event ID
    events[1], events[2] = events[2], events[1]  # out-of-order arrival
    late = make_event("north-shop", "US", "order", "late-order", {"order_id": "late-order", "amount": 42, "currency": "USD", "email": "late@example.test"}, now - timedelta(hours=36))
    events.append(late)
    return events


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=24)
    args = parser.parse_args()
    if not 1 <= args.count <= 1_000_000:
        parser.error("count must be between 1 and 1000000")
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), "enable.idempotence": True})
    events = generate(args.count)
    for event in events:
        entity = event["payload"].get("order_id") or event["payload"].get("sku", event["event_id"])
        producer.produce("commerce.events.v1", key=f"{event['tenant_id']}:{entity}", value=canonical_json(event))
        producer.poll(0)
    pending = producer.flush(30)
    if pending:
        raise RuntimeError(f"{pending} messages not delivered")
    print(json.dumps({"published": len(events), "tenants": 2, "markets": 4}))


if __name__ == "__main__":
    main()
