"""Small, deterministic two-tenant event stream. Larger runs need --count."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import random
import re
from uuid import uuid5, NAMESPACE_URL

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


def generate(count: int = 24, seed: int = 17, run_id: str | None = None) -> list[dict]:
    rng = random.Random(seed)
    # Stable event IDs must imply stable payloads across a replay on the same day.
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    run_tag = run_id or now.strftime("%Y%m%d")
    events: list[dict] = []
    for i in range(count):
        tenant, market, currency = ("north-shop", "US", "USD") if i % 2 == 0 else ("harbor-store", "GB", "GBP")
        if i % 6 == 0:
            market = "IN" if tenant == "north-shop" else "US"
            currency = "INR" if market == "IN" else "USD"
        order_id = f"demo-{run_tag}-{i:06d}"
        amount = 7500 if i % 11 == 0 else rng.randint(20, 350)
        at = now - timedelta(seconds=i % 3600)
        order = make_event(tenant, market, "order", order_id, {"order_id": order_id, "amount": amount, "currency": currency, "email": f"buyer{i}@example.test"}, at)
        payment = make_event(tenant, market, "payment", order_id, {"order_id": order_id, "status": "captured", "amount": amount}, at + timedelta(seconds=10))
        shipment = make_event(tenant, market, "shipment", order_id, {"order_id": order_id, "status": "delayed" if i % 7 == 0 else "in_transit", "eta_at": (at + timedelta(days=2)).isoformat()}, at + timedelta(seconds=20))
        events.extend([order, payment, shipment])
    events.append(make_event("north-shop", "US", "inventory", f"sku-1-{run_tag}", {"sku": "sku-1", "available": 7}, now))
    events.append(make_event("harbor-store", "GB", "application", f"app-1-{run_tag}", {"level": "INFO", "message": "checkout healthy"}, now))
    events.append(events[0].copy())  # duplicate event ID
    events[1], events[2] = events[2], events[1]  # out-of-order arrival
    late_id = f"late-order-{run_tag}"
    late = make_event("north-shop", "US", "order", late_id, {"order_id": late_id, "amount": 42, "currency": "USD", "email": "late@example.test"}, now - timedelta(hours=36))
    events.append(late)
    return events


def main() -> None:
    from confluent_kafka import Producer
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=24)
    parser.add_argument("--run-id", help="unique run label; default is current UTC date for repeatable replay")
    args = parser.parse_args()
    if not 1 <= args.count <= 1_000_000:
        parser.error("count must be between 1 and 1000000")
    if args.run_id and not re.fullmatch(r"[a-z0-9-]{1,32}", args.run_id):
        parser.error("run-id must be a lowercase slug of at most 32 characters")
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), "enable.idempotence": True})
    events = generate(args.count, run_id=args.run_id)
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
