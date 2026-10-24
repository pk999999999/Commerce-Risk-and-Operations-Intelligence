"""Operator-only tenant onboarding command, run inside the API container."""
from __future__ import annotations

import argparse
import getpass
import re

from psycopg.types.json import Jsonb

from app.auth import password_hash
from app.store import connect


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a tenant and its first admin")
    parser.add_argument("tenant_id")
    parser.add_argument("name")
    parser.add_argument("admin_email")
    parser.add_argument("--market", action="append", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,62}", args.tenant_id):
        parser.error("tenant_id must be a lowercase slug")
    if any(not re.fullmatch(r"[A-Z]{2}", market) for market in args.market):
        parser.error("each market must be an ISO two-letter code")
    password = getpass.getpass("Initial admin password: ")
    if len(password) < 12:
        parser.error("admin password must have at least 12 characters")
    with connect() as db:
        with db.cursor() as cur:
            cur.execute("INSERT INTO tenants (id,name) VALUES (%s,%s)", (args.tenant_id, args.name))
            for market in sorted(set(args.market)):
                cur.execute("INSERT INTO markets (tenant_id,code) VALUES (%s,%s)", (args.tenant_id, market))
            cur.execute("INSERT INTO tenant_config (tenant_id,version,fraud_amount,daily_event_quota,actor) VALUES (%s,1,5000,100000,'onboard')", (args.tenant_id,))
            cur.execute("INSERT INTO users (tenant_id,email,password_hash,role) VALUES (%s,%s,%s,'admin')", (args.tenant_id, args.admin_email, password_hash(password)))
            cur.execute("INSERT INTO audit_logs (tenant_id,actor,action,subject,detail) VALUES (%s,'onboard','tenant.create',%s,%s)", (args.tenant_id, args.tenant_id, Jsonb({"markets": sorted(set(args.market))})))
    print(f"Created tenant {args.tenant_id}")


if __name__ == "__main__":
    main()
