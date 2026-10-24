import os
import json
from pathlib import Path
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker

from app.auth import make_token, password_hash, read_token, verify_password
from app.core import alert_id, alerts_for_event, validate_event, validate_kafka_record
from app.generator import generate
from app.lake import LakePaths


def order(**changes):
    event = {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "tenant_id": "north-shop",
        "market": "US",
        "event_type": "order",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": {"order_id": "o-1", "amount": 7000, "currency": "USD", "email": "a@example.test"},
    }
    event.update(changes)
    return event


class ContractTests(unittest.TestCase):
    def test_valid_order_and_additive_field(self):
        event = order()
        event["payload"]["new_optional_field"] = "allowed"
        self.assertEqual(validate_event(event), event)

    def test_invalid_version_tenant_amount_and_timestamp(self):
        for changed in [
            {"schema_version": 2},
            {"tenant_id": "North Shop"},
            {"occurred_at": "2026-10-24T12:00:00"},
            {"payload": {"order_id": "o-1", "amount": "NaN", "currency": "USD", "email": "a@example.test"}},
        ]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                validate_event(order(**changed))

    def test_alert_id_is_tenant_scoped_and_replay_stable(self):
        event = order()
        first = alerts_for_event(event, Decimal("5000"))[0]
        self.assertEqual(first["alert_id"], alerts_for_event(event, Decimal("5000"))[0]["alert_id"])
        self.assertNotEqual(alert_id("north-shop", "o-1", "high_amount"), alert_id("harbor-store", "o-1", "high_amount"))

    def test_kafka_key_must_match_tenant(self):
        event = order()
        self.assertEqual(validate_kafka_record("north-shop:o-1", json.dumps(event)), event)
        with self.assertRaises(ValueError):
            validate_kafka_record("harbor-store:o-1", json.dumps(event))

    def test_lake_paths_reject_unknown_layer(self):
        lake = LakePaths("/tmp/commerce")
        self.assertEqual(lake.path("raw"), "/tmp/commerce/raw")
        with self.assertRaises(ValueError):
            lake.path("missing")

    def test_demo_contains_required_patterns(self):
        events = generate(24)
        self.assertEqual(events, generate(24))
        self.assertNotEqual(events[0]["event_id"], generate(24, run_id="another-run")[0]["event_id"])
        schema = json.loads((Path(__file__).resolve().parents[1] / "schemas" / "event-v1.schema.json").read_text(encoding="utf-8"))
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        for event in events:
            validate_event(event)
            validator.validate(event)
        self.assertEqual({e["tenant_id"] for e in events}, {"north-shop", "harbor-store"})
        self.assertEqual({e["market"] for e in events}, {"US", "IN", "GB"})
        self.assertEqual({e["event_type"] for e in events}, {"order", "payment", "shipment", "inventory", "application"})
        self.assertLess(len({e["event_id"] for e in events}), len(events))
        self.assertTrue(any(e["event_type"] == "shipment" and e["payload"]["status"] == "delayed" for e in events))
        self.assertTrue(any(e["event_type"] == "order" and e["payload"]["amount"] >= 5000 for e in events))
        self.assertEqual([e for e in events if e["payload"].get("order_id", "").startswith("late-order-")][0]["tenant_id"], "north-shop")


class AuthTests(unittest.TestCase):
    def setUp(self):
        os.environ["TOKEN_SECRET"] = "unit-test-secret-is-over-thirty-two-characters"

    def test_password_hash_and_token_signature(self):
        stored = password_hash("long secure password")
        self.assertTrue(verify_password("long secure password", stored))
        self.assertFalse(verify_password("wrong", stored))
        token = make_token(1, "north-shop", "viewer")
        self.assertEqual(read_token(token)["tenant"], "north-shop")
        with self.assertRaises(ValueError):
            read_token(token + "tampered")

    def test_expired_token(self):
        with self.assertRaises(ValueError):
            read_token(make_token(1, "north-shop", "viewer", ttl=-1))


if __name__ == "__main__":
    unittest.main()
