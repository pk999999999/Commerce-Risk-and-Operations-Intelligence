"""Run after Compose is healthy with COMMERCE_INTEGRATION_URL set."""
import json
import os
import subprocess
import time
import unittest
import urllib.error
import urllib.request

BASE = os.getenv("COMMERCE_INTEGRATION_URL")


def request(path, token=None, body=None, method="GET", api_key=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if api_key:
        headers["X-API-Key"] = api_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw) if response.headers.get_content_type() == "application/json" else raw.decode()
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


@unittest.skipUnless(BASE, "Set COMMERCE_INTEGRATION_URL for Compose integration tests")
class TenantIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        password = os.environ["DEMO_ADMIN_PASSWORD"]
        cls.a = request("/auth/login", body={"tenant_id": "north-shop", "email": "admin@north.test", "password": password}, method="POST")[1]["access_token"]
        cls.b = request("/auth/login", body={"tenant_id": "harbor-store", "email": "admin@harbor.test", "password": password}, method="POST")[1]["access_token"]
        for _ in range(30):
            if request("/api/alerts", cls.a)[1] and request("/api/alerts", cls.b)[1]:
                return
            time.sleep(3)
        raise AssertionError("demo alerts did not arrive within 90 seconds")

    def test_alert_and_order_boundaries(self):
        a_alerts = request("/api/alerts", self.a)[1]
        b_alerts = request("/api/alerts", self.b)[1]
        self.assertTrue(a_alerts)
        self.assertTrue(b_alerts)
        self.assertEqual(request("/api/alerts/" + b_alerts[0]["alert_id"], self.a)[0], 404)
        self.assertEqual(request("/api/alerts/" + a_alerts[0]["alert_id"], self.b)[0], 404)
        self.assertEqual(request("/api/alerts/" + b_alerts[0]["alert_id"] + "/status", self.a, {"status": "closed"}, "PATCH")[0], 404)
        a_orders = request("/api/orders", self.a)[1]
        b_orders = request("/api/orders", self.b)[1]
        self.assertTrue(set(o["order_id"] for o in a_orders).isdisjoint(o["order_id"] for o in b_orders))
        status, csv_text = request("/api/export/orders.csv", self.a)
        self.assertEqual(status, 200)
        self.assertNotIn(b_orders[0]["order_id"], csv_text)

    def test_authentication_required(self):
        self.assertEqual(request("/api/alerts")[0], 401)
        self.assertEqual(request("/api/analytics")[0], 401)

    def test_api_key_scope_and_revocation(self):
        status, created = request("/api/admin/api-keys", self.a, {"label": "isolation-test", "role": "viewer"}, "POST")
        self.assertEqual(status, 200)
        secret = created["api_key"]
        self.assertEqual(request("/api/alerts", api_key=secret)[0], 200)
        b_alert = request("/api/alerts", self.b)[1][0]
        self.assertEqual(request("/api/alerts/" + b_alert["alert_id"], api_key=secret)[0], 404)
        self.assertEqual(request("/api/admin/api-keys/" + str(created["id"]), self.a, method="DELETE")[0], 200)
        self.assertEqual(request("/api/alerts", api_key=secret)[0], 401)

    def test_search_is_tenant_scoped(self):
        b_alerts = request("/api/alerts", self.b)[1]
        self.assertTrue(b_alerts)
        order_id = b_alerts[0]["order_id"]
        for _ in range(20):
            status, own_results = request("/api/alerts/search?q=" + order_id, self.b)
            if status == 200 and own_results:
                break
            time.sleep(3)
        self.assertEqual(status, 200)
        self.assertTrue(own_results)
        status, other_results = request("/api/alerts/search?q=" + order_id, self.a)
        self.assertEqual(status, 200)
        self.assertEqual(other_results, [])


@unittest.skipUnless(BASE and os.getenv("COMMERCE_REPLAY_TEST") == "1", "Explicitly set COMMERCE_REPLAY_TEST=1 for replay and restart tests")
class ReplayTests(unittest.TestCase):
    def test_replay_and_processor_restart(self):
        password = os.environ["DEMO_ADMIN_PASSWORD"]
        token = request("/auth/login", body={"tenant_id": "north-shop", "email": "admin@north.test", "password": password}, method="POST")[1]["access_token"]
        before_orders = {row["order_id"] for row in request("/api/orders?limit=100", token)[1]}
        before_alerts = {row["alert_id"] for row in request("/api/alerts?limit=100", token)[1]}
        subprocess.run(["docker", "compose", "run", "--rm", "generator"], check=True, timeout=180)
        time.sleep(15)
        self.assertEqual(before_orders, {row["order_id"] for row in request("/api/orders?limit=100", token)[1]})
        self.assertEqual(before_alerts, {row["alert_id"] for row in request("/api/alerts?limit=100", token)[1]})
        subprocess.run(["docker", "compose", "restart", "processor"], check=True, timeout=180)
        unique_run = "recovery-" + str(int(time.time()))
        subprocess.run(["docker", "compose", "run", "--rm", "generator", "python", "-m", "app.generator", "--count", "4", "--run-id", unique_run], check=True, timeout=180)
        for _ in range(30):
            after_orders = {row["order_id"] for row in request("/api/orders?limit=100", token)[1]}
            if len(after_orders) > len(before_orders):
                break
            time.sleep(3)
        self.assertGreater(len(after_orders), len(before_orders))


if __name__ == "__main__":
    unittest.main()
