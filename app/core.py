"""Versioned event contract and deterministic alert rules."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from uuid import UUID

EVENT_TYPES = {"order", "payment", "inventory", "shipment", "application"}
TENANT_RE = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
REQUIRED = {
    "order": {"order_id", "amount", "currency", "email"},
    "payment": {"order_id", "status", "amount"},
    "inventory": {"sku", "available"},
    "shipment": {"order_id", "status", "eta_at"},
    "application": {"level", "message"},
}


def parse_time(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must include UTC offset")
    return timestamp.astimezone(timezone.utc)


def validate_event(event: object) -> dict:
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    if event.get("schema_version") != 1:
        raise ValueError("unsupported schema_version")
    try:
        UUID(str(event["event_id"]))
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError("event_id must be a UUID") from exc
    tenant_id = event.get("tenant_id")
    if not isinstance(tenant_id, str) or not TENANT_RE.fullmatch(tenant_id):
        raise ValueError("invalid tenant_id")
    market = event.get("market")
    if not isinstance(market, str) or not re.fullmatch(r"[A-Z]{2}", market):
        raise ValueError("market must be an ISO two-letter code")
    event_type = event.get("event_type")
    if event_type not in EVENT_TYPES:
        raise ValueError("unsupported event_type")
    try:
        parse_time(event["occurred_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("invalid occurred_at") from exc
    payload = event.get("payload")
    if not isinstance(payload, dict) or not REQUIRED[event_type].issubset(payload):
        raise ValueError("missing payload fields")
    if event_type in {"order", "payment", "shipment"}:
        if not isinstance(payload["order_id"], str) or not 1 <= len(payload["order_id"]) <= 128:
            raise ValueError("invalid order_id")
    if event_type in {"order", "payment"}:
        try:
            amount = Decimal(str(payload["amount"]))
            if not amount.is_finite() or amount < 0 or amount > Decimal("1000000000000"):
                raise ValueError("amount out of range")
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("invalid amount") from exc
    if event_type == "order":
        if not isinstance(payload["currency"], str) or not re.fullmatch(r"[A-Z]{3}", payload["currency"]):
            raise ValueError("invalid currency")
        if not isinstance(payload["email"], str) or len(payload["email"]) > 320 or "@" not in payload["email"]:
            raise ValueError("invalid email")
    if event_type == "payment" and payload["status"] not in {"authorized", "captured", "failed", "refunded"}:
        raise ValueError("invalid payment status")
    if event_type == "shipment":
        if payload["status"] not in {"pending", "in_transit", "delayed", "delivered"}:
            raise ValueError("invalid shipment status")
        parse_time(payload["eta_at"])
    if event_type == "inventory" and (not isinstance(payload["sku"], str) or not payload["sku"] or not isinstance(payload["available"], int) or payload["available"] < 0):
        raise ValueError("invalid inventory payload")
    if event_type == "application" and (payload["level"] not in {"DEBUG", "INFO", "WARN", "ERROR"} or not isinstance(payload["message"], str)):
        raise ValueError("invalid application payload")
    return event


def alert_id(tenant_id: str, order_id: str, rule: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{order_id}:{rule}".encode()).hexdigest()[:32]


def alerts_for_event(event: dict, fraud_amount: Decimal) -> list[dict]:
    payload = event["payload"]
    event_type = event["event_type"]
    alerts: list[dict] = []
    if event_type == "order" and Decimal(str(payload["amount"])) >= fraud_amount:
        alerts.append({"rule": "high_amount", "severity": "high", "order_id": payload["order_id"]})
    if event_type == "shipment" and payload["status"] == "delayed":
        alerts.append({"rule": "shipment_delayed", "severity": "medium", "order_id": payload["order_id"]})
    for alert in alerts:
        alert["alert_id"] = alert_id(event["tenant_id"], alert["order_id"], alert["rule"])
        alert["evidence"] = {
            "source_event_id": event["event_id"],
            "occurred_at": event["occurred_at"],
            "market": event["market"],
            "payload": payload,
        }
    return alerts


def canonical_json(event: dict) -> str:
    return json.dumps(event, separators=(",", ":"), sort_keys=True)


def validate_kafka_record(key: str, value: str) -> dict:
    event = validate_event(json.loads(value))
    if not key or key.split(":", 1)[0] != event["tenant_id"]:
        raise ValueError("Kafka key tenant does not match payload tenant")
    return event
