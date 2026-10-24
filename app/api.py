"""Tenant-scoped API and browser UI for the local commerce platform."""
from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal
import csv
import hashlib
import io
import os
import secrets

import clickhouse_connect
from elasticsearch import Elasticsearch
from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb

from app.auth import make_token, password_hash, read_token, verify_password
from app.store import connect, seed_demo


@asynccontextmanager
async def lifespan(app: FastAPI):
    seed_demo()
    yield


app = FastAPI(title="Commerce Risk and Operations Intelligence", lifespan=lifespan)


class Login(BaseModel):
    tenant_id: str
    email: str
    password: str


class AlertStatus(BaseModel):
    status: str = Field(pattern="^(open|investigating|closed)$")


class NewUser(BaseModel):
    email: str
    password: str = Field(min_length=12)
    role: str = Field(pattern="^(admin|investigator|viewer)$")


class NewConfig(BaseModel):
    fraud_amount: Decimal = Field(gt=0, le=1_000_000_000)
    daily_event_quota: int = Field(gt=0, le=100_000_000)


class NewKey(BaseModel):
    label: str = Field(min_length=1, max_length=100)
    role: str = Field(pattern="^(admin|investigator|viewer)$")


def principal(authorization: str | None = Header(default=None), x_api_key: str | None = Header(default=None)) -> dict:
    with connect() as db:
        with db.cursor() as cur:
            if x_api_key:
                key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
                cur.execute("SELECT tenant_id,role,label FROM api_keys WHERE key_hash=%s AND revoked_at IS NULL", (key_hash,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(401, "invalid API key")
                actor = f"key:{row['label']}"
            elif authorization and authorization.startswith("Bearer "):
                try:
                    token = read_token(authorization[7:])
                except ValueError:
                    raise HTTPException(401, "invalid bearer token") from None
                cur.execute("SELECT tenant_id,role,email FROM users WHERE id=%s AND tenant_id=%s AND active=true", (token["sub"], token["tenant"]))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(401, "user disabled")
                actor = row["email"]
            else:
                raise HTTPException(401, "authentication required")
            tenant = row["tenant_id"]
            cur.execute("INSERT INTO usage_daily (tenant_id,day,api_count) VALUES (%s,current_date,1) ON CONFLICT (tenant_id,day) DO UPDATE SET api_count=usage_daily.api_count+1 RETURNING api_count", (tenant,))
            used = cur.fetchone()["api_count"]
            cur.execute("SELECT daily_event_quota FROM tenant_config WHERE tenant_id=%s ORDER BY version DESC LIMIT 1", (tenant,))
            config = cur.fetchone()
            if config and used > config["daily_event_quota"] * 10:
                raise HTTPException(429, "daily API quota exceeded")
            return {"tenant_id": tenant, "role": row["role"], "actor": actor}


def require_role(user: dict, allowed: set[str]) -> None:
    if user["role"] not in allowed:
        raise HTTPException(403, "insufficient role")


def audit(cur, user: dict, action: str, subject: str, detail: dict | None = None) -> None:
    cur.execute("INSERT INTO audit_logs (tenant_id,actor,action,subject,detail) VALUES (%s,%s,%s,%s,%s)", (user["tenant_id"], user["actor"], action, subject, Jsonb(detail or {})))


@app.get("/")
def home():
    return FileResponse("web/index.html")


@app.get("/health")
def health():
    with connect() as db:
        db.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/auth/login")
def login(body: Login):
    with connect() as db:
        row = db.execute("SELECT id,tenant_id,role,password_hash FROM users WHERE tenant_id=%s AND email=%s AND active=true", (body.tenant_id, body.email)).fetchone()
    if not row or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(401, "invalid credentials")
    return {"access_token": make_token(row["id"], row["tenant_id"], row["role"]), "token_type": "bearer"}


@app.get("/api/me")
def me(user: dict = Depends(principal)):
    return user


@app.get("/api/alerts")
def alerts(limit: int = 50, user: dict = Depends(principal)):
    limit = max(1, min(limit, 100))
    with connect() as db:
        rows = db.execute("SELECT alert_id,order_id,rule,severity,status,evidence,created_at FROM alerts WHERE tenant_id=%s ORDER BY created_at DESC LIMIT %s", (user["tenant_id"], limit)).fetchall()
    return rows


@app.get("/api/alerts/search")
def search_alerts(q: str, user: dict = Depends(principal)):
    if len(q) > 200:
        raise HTTPException(400, "query too long")
    es = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"))
    try:
        result = es.search(index="commerce-alerts-v1", query={"bool": {"filter": [{"term": {"tenant_id.keyword": user["tenant_id"]}}], "must": [{"multi_match": {"query": q, "fields": ["order_id", "rule", "evidence.payload.email"]}}]}}, size=50)
    except Exception:
        raise HTTPException(503, "search temporarily unavailable") from None
    return [hit["_source"] for hit in result["hits"]["hits"]]


@app.get("/api/alerts/{alert_id}")
def alert_detail(alert_id: str, user: dict = Depends(principal)):
    with connect() as db:
        row = db.execute("SELECT alert_id,order_id,rule,severity,status,evidence,created_at,updated_at FROM alerts WHERE tenant_id=%s AND alert_id=%s", (user["tenant_id"], alert_id)).fetchone()
    if not row:
        raise HTTPException(404, "alert not found")
    return row


@app.patch("/api/alerts/{alert_id}/status")
def update_alert(alert_id: str, body: AlertStatus, user: dict = Depends(principal)):
    require_role(user, {"admin", "investigator"})
    with connect() as db:
        with db.cursor() as cur:
            cur.execute("UPDATE alerts SET status=%s,updated_at=now() WHERE tenant_id=%s AND alert_id=%s RETURNING alert_id,status", (body.status, user["tenant_id"], alert_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "alert not found")
            audit(cur, user, "alert.status", alert_id, {"status": body.status})
    return row


@app.get("/api/orders")
def orders(limit: int = 50, user: dict = Depends(principal)):
    limit = max(1, min(limit, 100))
    with connect() as db:
        return db.execute("SELECT order_id,market,amount,currency,payment_status,shipment_status,eta_at,order_at FROM orders WHERE tenant_id=%s ORDER BY order_at DESC NULLS LAST LIMIT %s", (user["tenant_id"], limit)).fetchall()


@app.get("/api/analytics")
def analytics(user: dict = Depends(principal)):
    ch = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), username="commerce", password=os.environ["CLICKHOUSE_PASSWORD"])
    result = ch.query("SELECT market, count() AS orders, sum(amount) AS gross FROM commerce.order_facts FINAL WHERE tenant_id = {tenant:String} GROUP BY market ORDER BY market", parameters={"tenant": user["tenant_id"]})
    return [{"market": row[0], "orders": row[1], "gross": str(row[2])} for row in result.result_rows]


@app.get("/api/export/orders.csv")
def export_orders(user: dict = Depends(principal)):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["order_id", "market", "amount", "currency", "order_at"])
    with connect() as db:
        for row in db.execute("SELECT order_id,market,amount,currency,order_at FROM orders WHERE tenant_id=%s ORDER BY order_id", (user["tenant_id"],)):
            writer.writerow([row["order_id"], row["market"], row["amount"], row["currency"], row["order_at"]])
    return Response(output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=orders.csv"})


@app.post("/api/admin/users")
def add_user(body: NewUser, user: dict = Depends(principal)):
    require_role(user, {"admin"})
    with connect() as db:
        with db.cursor() as cur:
            cur.execute("INSERT INTO users (tenant_id,email,password_hash,role) VALUES (%s,%s,%s,%s) RETURNING id,email,role", (user["tenant_id"], body.email, password_hash(body.password), body.role))
            row = cur.fetchone()
            audit(cur, user, "user.create", body.email, {"role": body.role})
    return row


@app.post("/api/admin/api-keys")
def add_key(body: NewKey, user: dict = Depends(principal)):
    require_role(user, {"admin"})
    secret = "cri_" + secrets.token_urlsafe(32)
    with connect() as db:
        with db.cursor() as cur:
            cur.execute("INSERT INTO api_keys (tenant_id,label,key_hash,role) VALUES (%s,%s,%s,%s) RETURNING id", (user["tenant_id"], body.label, hashlib.sha256(secret.encode()).hexdigest(), body.role))
            row = cur.fetchone()
            audit(cur, user, "api_key.create", str(row["id"]), {"label": body.label, "role": body.role})
    return {"id": row["id"], "api_key": secret}


@app.post("/api/admin/config")
def add_config(body: NewConfig, user: dict = Depends(principal)):
    require_role(user, {"admin"})
    with connect() as db:
        with db.cursor() as cur:
            cur.execute("SELECT id FROM tenants WHERE id=%s FOR UPDATE", (user["tenant_id"],))
            cur.execute("SELECT COALESCE(max(version),0)+1 AS next_version FROM tenant_config WHERE tenant_id=%s", (user["tenant_id"],))
            version = cur.fetchone()["next_version"]
            cur.execute("INSERT INTO tenant_config (tenant_id,version,fraud_amount,daily_event_quota,actor) VALUES (%s,%s,%s,%s,%s)", (user["tenant_id"], version, body.fraud_amount, body.daily_event_quota, user["actor"]))
            audit(cur, user, "config.update", str(version), body.model_dump(mode="json"))
    return {"version": version}


@app.get("/api/admin/config")
def config_history(user: dict = Depends(principal)):
    require_role(user, {"admin"})
    with connect() as db:
        return db.execute("SELECT version,fraud_amount,daily_event_quota,actor,created_at FROM tenant_config WHERE tenant_id=%s ORDER BY version DESC", (user["tenant_id"],)).fetchall()
