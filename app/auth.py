"""Small dependency-free password and bearer token implementation."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import secrets
import time


def password_hash(password: str, salt: bytes | None = None) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)
    return f"pbkdf2_sha256$310000${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algorithm, rounds, salt, digest = stored.split("$")
        if algorithm != "pbkdf2_sha256":
            return False
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(rounds))
        return hmac.compare_digest(candidate, bytes.fromhex(digest))
    except (ValueError, TypeError):
        return False


def _secret() -> bytes:
    value = os.environ.get("TOKEN_SECRET", "")
    if len(value) < 32:
        raise RuntimeError("TOKEN_SECRET must be at least 32 characters")
    return value.encode()


def make_token(user_id: int, tenant_id: str, role: str, ttl: int = 28_800) -> str:
    payload = {"sub": user_id, "tenant": tenant_id, "role": role, "exp": int(time.time()) + ttl}
    body = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).rstrip(b"=")
    signature = hmac.new(_secret(), body, hashlib.sha256).digest()
    return body.decode() + "." + base64.urlsafe_b64encode(signature).rstrip(b"=").decode()


def read_token(token: str) -> dict:
    try:
        body, supplied = token.split(".", 1)
        expected = hmac.new(_secret(), body.encode(), hashlib.sha256).digest()
        signature = base64.urlsafe_b64decode(supplied + "=" * (-len(supplied) % 4))
        if not hmac.compare_digest(expected, signature):
            raise ValueError("bad signature")
        data = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        if data["exp"] <= time.time():
            raise ValueError("expired")
        return data
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise ValueError("invalid token") from exc
