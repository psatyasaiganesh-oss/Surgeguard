"""Purpose-separated, database-bound tokens; not an external identity provider."""

import base64
import hashlib
import hmac
import re

from .model import Problem, canonical, fields, identifier, integer, parse_json, reference, require


def encode(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode(text):
    require(isinstance(text, str) and re.fullmatch(r"[A-Za-z0-9_-]+", text), "invalid token",
            "invalid_token", 401)
    try:
        raw = base64.b64decode(text + "=" * (-len(text) % 4), altchars=b"-_", validate=True)
        require(encode(raw) == text, "noncanonical token", "invalid_token", 401)
        return raw
    except (ValueError, TypeError) as exc:
        raise Problem("invalid_token", "invalid token", 401) from exc


class Tokens:
    def __init__(self, secret, database_id, clock):
        self.secret, self.database_id, self.clock = secret, database_id, clock

    def sign(self, purpose, claims):
        data = encode(canonical({"v": 1, "db": self.database_id, **claims}).encode())
        mac = hmac.new(self.secret, ("scopegraph/v1/" + purpose + "." + data).encode(),
                       hashlib.sha256).digest()
        return data + "." + encode(mac)

    def verify(self, purpose, token):
        try:
            require(isinstance(token, str) and len(token) <= 2048 and token.count(".") == 1,
                    "invalid token")
            data, signature = token.split(".")
            payload = decode(data)
            expected = hmac.new(self.secret,
                                ("scopegraph/v1/" + purpose + "." + data).encode(),
                                hashlib.sha256).digest()
            require(hmac.compare_digest(decode(signature), expected), "invalid token")
            claims = parse_json(payload)
            require(isinstance(claims, dict) and claims.get("v") == 1
                    and type(claims["v"]) is int and claims.get("db") == self.database_id,
                    "invalid token")
            return claims
        except Problem as exc:
            raise Problem("invalid_token", "invalid or expired token", 401) from exc

    def revision(self, tenant, revision):
        return self.sign("revision", {"tenant": tenant, "revision": revision})

    def minimum(self, token, tenant):
        if token is None:
            return 0
        claims = self.verify("revision", token)
        try:
            fields(claims, ("v", "db", "tenant", "revision"))
            require(claims["tenant"] == tenant, "token tenant mismatch")
            return integer(claims["revision"])
        except Problem as exc:
            raise Problem("invalid_token", "invalid revision token", 401) from exc

    def mint(self, tenant, user, role="user", ttl=900):
        identifier(tenant)
        reference(user, ("user",))
        require(role in ("user", "admin"), "invalid role")
        integer(ttl, 1, 3600)
        now = int(self.clock())
        return self.sign("access", {"tenant": tenant, "sub": user, "role": role,
                                    "iat": now, "exp": now + ttl})

    def authenticate(self, token):
        claims = self.verify("access", token)
        try:
            fields(claims, ("v", "db", "tenant", "sub", "role", "iat", "exp"))
            identifier(claims["tenant"])
            reference(claims["sub"], ("user",))
            require(claims["role"] in ("user", "admin"), "invalid role")
            integer(claims["iat"])
            integer(claims["exp"])
            now = self.clock()
            require(claims["iat"] <= now < claims["exp"]
                    and 0 < claims["exp"] - claims["iat"] <= 3600, "expired token")
            return claims
        except Problem as exc:
            raise Problem("invalid_token", "invalid or expired token", 401) from exc
