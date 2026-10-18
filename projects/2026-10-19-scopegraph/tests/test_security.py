import os
import sqlite3
import stat
from pathlib import Path

from scopegraph.engine import Engine
from scopegraph.model import parse_json
from .helpers import Fixture, create, edge


class TokenTests(Fixture):
    def test_access_token_identity_tenant_and_role(self):
        token = self.engine.mint("acme", "user:alice", "admin", 30)
        claims = self.engine.tokens.authenticate(token)
        self.assertEqual(("acme", "user:alice", "admin"), (claims["tenant"], claims["sub"], claims["role"]))

    def test_access_token_expires_at_exact_boundary(self):
        token = self.engine.mint("acme", "user:alice", ttl=10)
        self.clock.now += 10
        self.problem("invalid_token", self.engine.tokens.authenticate, token)

    def test_future_access_token_rejected(self):
        token = self.engine.mint("acme", "user:alice")
        self.clock.now -= 1
        self.problem("invalid_token", self.engine.tokens.authenticate, token)

    def test_signature_tampering_and_malformed_tokens(self):
        token = self.engine.mint("acme", "user:alice")
        for invalid in (token[:-5] + "AAAAA", "", "a.b.c", "=.a", "a.b", 1, "x" * 3000):
            with self.subTest(kind=type(invalid).__name__):
                self.problem("invalid_token", self.engine.tokens.authenticate, invalid)

    def test_revision_token_cannot_authenticate(self):
        self.problem("invalid_token", self.engine.tokens.authenticate, self.seed["revision_token"])

    def test_access_token_cannot_be_minimum_revision(self):
        self.problem("invalid_token", self.check, min_token=self.engine.mint("acme", "user:alice"))

    def test_cross_database_token_is_rejected(self):
        other = Engine(Path(self.temp.name) / "other.db", clock=self.clock)
        other.create_tenant("acme")
        self.problem("invalid_token", other.tokens.authenticate, self.engine.mint("acme", "user:alice"))

    def test_cross_tenant_revision_token_rejected(self):
        self.engine.create_tenant("beta")
        self.problem("invalid_token", self.engine.check, "beta", "document:plan", "view", "user:alice",
                     self.seed["revision_token"])

    def test_signed_but_invalid_revision_claim_rejected(self):
        for value in (True, -1, 1.5, "2"):
            with self.subTest(value=value):
                token = self.engine.tokens.sign("revision", {"tenant": "acme", "revision": value})
                self.problem("invalid_token", self.check, min_token=token)

    def test_signed_unknown_access_claim_rejected(self):
        claims = {"tenant": "acme", "sub": "user:alice", "role": "user", "iat": int(self.clock()),
                  "exp": int(self.clock()) + 30, "extra": "not allowed"}
        self.problem("invalid_token", self.engine.tokens.authenticate, self.engine.tokens.sign("access", claims))

    def test_mint_limits(self):
        for ttl in (0, 3601, True):
            self.problem("invalid_input", self.engine.mint, "acme", "user:alice", ttl=ttl)
        self.problem("invalid_input", self.engine.mint, "acme", "group:eng#member")
        self.problem("invalid_input", self.engine.mint, "acme", "user:alice", role="root")


class ValidationTests(Fixture):
    def test_database_and_sidecars_private(self):
        self.assertEqual(0o600, stat.S_IMODE(os.stat(self.path).st_mode))
        with self.engine._tx(True) as conn:
            conn.execute("UPDATE tenants SET revision=revision WHERE id='acme'")
            for suffix in ("-wal", "-shm"):
                path = Path(str(self.path) + suffix)
                if path.exists():
                    self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))

    def test_symlink_database_rejected(self):
        link = Path(self.temp.name) / "link.db"
        link.symlink_to(self.path)
        self.problem("invalid_input", Engine, link)

    def test_unrelated_database_rejected_without_modification(self):
        other = Path(self.temp.name) / "foreign.db"
        with sqlite3.connect(other) as conn:
            conn.execute("CREATE TABLE important(value TEXT)")
            conn.execute("INSERT INTO important VALUES('preserve')")
        other.chmod(0o600)
        self.problem("invalid_input", Engine, other)
        with sqlite3.connect(other) as conn:
            self.assertEqual("preserve", conn.execute("SELECT value FROM important").fetchone()[0])

    def test_world_readable_database_rejected(self):
        self.path.chmod(0o644)
        self.problem("invalid_input", Engine, self.path)
        self.path.chmod(0o600)

    def test_strict_json(self):
        invalid = [b'{"a":1,"a":2}', b'{"x":NaN}', b'{"x":1e999}', b'\xff',
                   b'{} trailing', ('[' * 18 + '0' + ']' * 18).encode(), b'x' * 65537,
                   '{}'.encode("utf-16")]
        for raw in invalid:
            with self.subTest(length=len(raw)):
                self.problem("invalid_input", parse_json, raw)

    def test_batch_limits_and_unknown_fields(self):
        self.problem("invalid_input", self.write, [])
        self.problem("invalid_input", self.write, [create("document:x")] * 201)
        self.problem("invalid_input", self.write, [{**create("document:x"), "admin": True}])

    def test_fixed_relation_schema(self):
        cases = [edge("document:plan", "execute", "user:alice"),
                 edge("group:eng", "owner", "user:alice"),
                 edge("document:plan", "viewer", "group:eng#owner"),
                 edge("document:plan", "parent", "document:other"),
                 edge("document:plan", "viewer", "user:*"),
                 edge("document:plan", "viewer", "user:alice", expires_at=True)]
        for case in cases:
            self.problem("invalid_input", self.write, [case])

    def test_nonfinite_expiry_and_boolean_cas_rejected(self):
        for expiry in (float("nan"), float("inf"), -1):
            self.problem("invalid_input", self.write, [edge("document:plan", "viewer", "user:a", expires_at=expiry)])
        self.problem("invalid_input", self.write, [create("document:x")], expected_revision=True)

    def test_unknown_permission_and_subject_rejected(self):
        self.problem("invalid_input", self.check, perm="admin")
        self.problem("invalid_input", self.check, user="group:eng#member")

    def test_unknown_tenant_is_not_silently_created(self):
        self.problem("not_found", self.engine.check, "missing", "document:plan", "view", "user:alice")
