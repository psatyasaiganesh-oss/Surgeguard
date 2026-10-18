"""Durable graph writes, revision-aware cache, and atomic protected content access."""

import hashlib
import os
import secrets
import sqlite3
import stat
import threading
import time
from collections import OrderedDict, deque
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

from .model import (Problem, canonical, identifier, integer, operation, parse_json,
                    permission, reference, require)
from .policy import Evaluator
from .tokens import Tokens

APPLICATION_ID = 0x53434731
SCHEMA = (
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE tenants(id TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK(revision>=0))",
    "CREATE TABLE objects(tenant TEXT NOT NULL, ref TEXT NOT NULL, PRIMARY KEY(tenant,ref), "
    "FOREIGN KEY(tenant) REFERENCES tenants(id))",
    "CREATE TABLE tuples(tenant TEXT NOT NULL, resource TEXT NOT NULL, relation TEXT NOT NULL, "
    "subject TEXT NOT NULL, expires_at REAL, PRIMARY KEY(tenant,resource,relation,subject), "
    "FOREIGN KEY(tenant,resource) REFERENCES objects(tenant,ref) ON DELETE CASCADE)",
    "CREATE INDEX expiry ON tuples(tenant,expires_at) WHERE expires_at IS NOT NULL",
    "CREATE INDEX incoming ON tuples(tenant,subject)",
    "CREATE TABLE contents(tenant TEXT NOT NULL, resource TEXT NOT NULL, body TEXT NOT NULL, "
    "version INTEGER NOT NULL, acl_revision INTEGER NOT NULL, PRIMARY KEY(tenant,resource), "
    "FOREIGN KEY(tenant,resource) REFERENCES objects(tenant,ref) ON DELETE CASCADE)",
    "CREATE TABLE receipts(tenant TEXT NOT NULL, action TEXT NOT NULL, request_id TEXT NOT NULL, "
    "digest TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(tenant,action,request_id), "
    "FOREIGN KEY(tenant) REFERENCES tenants(id))",
    "CREATE TABLE audit(tenant TEXT NOT NULL, revision INTEGER NOT NULL, request_id TEXT NOT NULL, "
    "operations INTEGER NOT NULL, PRIMARY KEY(tenant,revision), "
    "FOREIGN KEY(tenant) REFERENCES tenants(id))",
)


class Engine:
    def __init__(self, path, *, clock=time.time, cache_size=2048, cache_ttl=60,
                 max_edges=2000, max_depth=32):
        require(str(path) != ":memory:", "use a file-backed database")
        integer(cache_size, 0, 10000)
        integer(cache_ttl, 1, 3600)
        integer(max_edges, 1, 100000)
        integer(max_depth, 1, 128)
        self.path, self.clock = str(Path(path).absolute()), clock
        self.cache_size, self.cache_ttl = cache_size, cache_ttl
        self.max_edges, self.max_depth = max_edges, max_depth
        self.cache, self.lock = OrderedDict(), threading.Lock()
        self.hits = self.misses = self.evictions = 0
        self.last_time = float("-inf")
        self._initialize()
        with self._tx() as conn:
            meta = dict(conn.execute("SELECT key,value FROM meta"))
        self.tokens = Tokens(bytes.fromhex(meta["secret"]), meta["database_id"], clock)

    def _initialize(self):
        created = False
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            created = True
        except FileExistsError:
            info = os.lstat(self.path)
            require(stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode),
                    "database must be a regular non-symlink file")
            require(stat.S_IMODE(info.st_mode) & 0o077 == 0,
                    "database permissions must be private (0600)")
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        try:
            if created:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                for statement in SCHEMA:
                    conn.execute(statement)
                conn.execute("INSERT INTO meta VALUES('secret',?)", (secrets.token_hex(32),))
                conn.execute("INSERT INTO meta VALUES('database_id',?)", (secrets.token_hex(16),))
                conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
                conn.execute("PRAGMA user_version=1")
                conn.commit()
            else:
                require(conn.execute("PRAGMA application_id").fetchone()[0] == APPLICATION_ID
                        and conn.execute("PRAGMA user_version").fetchone()[0] == 1,
                        "not a supported ScopeGraph database")
                require(conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal",
                        "database must use WAL")
        finally:
            conn.close()

    @contextmanager
    def _tx(self, write=False):
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.commit()
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                raise Problem("busy", "database busy; retry with the same request ID", 503) from exc
            raise
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _revision(conn, tenant, minimum=0):
        row = conn.execute("SELECT revision FROM tenants WHERE id=?", (tenant,)).fetchone()
        require(row is not None, "tenant not found", "not_found", 404)
        require(row[0] >= minimum, "required revision is not available", "revision_unavailable", 409)
        return row[0]

    def create_tenant(self, tenant):
        identifier(tenant)
        with self._tx(True) as conn:
            conn.execute("INSERT OR IGNORE INTO tenants VALUES(?,0)", (tenant,))
            revision = self._revision(conn, tenant)
        return {"tenant": tenant, "revision": revision}

    def mint(self, tenant, user, role="user", ttl=900):
        identifier(tenant)
        with self._tx() as conn:
            self._revision(conn, tenant)
        return self.tokens.mint(tenant, user, role, ttl)

    @staticmethod
    def _receipt(conn, tenant, action, request_id, digest):
        row = conn.execute("SELECT digest,result FROM receipts WHERE tenant=? AND action=? "
                           "AND request_id=?", (tenant, action, request_id)).fetchone()
        if row:
            require(row[0] == digest, "request ID already used for different input", "conflict", 409)
            return {**parse_json(row[1]), "replayed": True}
        return None

    @staticmethod
    def _save_receipt(conn, tenant, action, request_id, digest, result):
        conn.execute("INSERT INTO receipts VALUES(?,?,?,?,?)",
                     (tenant, action, request_id, digest, canonical(result)))

    @staticmethod
    def _exists(conn, tenant, ref):
        require(conn.execute("SELECT 1 FROM objects WHERE tenant=? AND ref=?",
                             (tenant, ref)).fetchone() is not None,
                "referenced object does not exist", "not_found", 404)

    @staticmethod
    def _acyclic(conn, tenant):
        # Validate the final batch, including expired links. No recursive SQL or Python.
        adjacency, indegree = {}, {}
        for row in conn.execute("SELECT resource,relation,subject FROM tuples WHERE tenant=? "
                                "AND (relation='parent' OR relation='member')", (tenant,)):
            resource, rel, who = row
            target = who if rel == "parent" else who[:-7] if who.endswith("#member") else None
            if target is not None:
                adjacency.setdefault(resource, []).append(target)
                indegree.setdefault(resource, 0)
                indegree[target] = indegree.get(target, 0) + 1
        queue = deque(node for node, degree in indegree.items() if degree == 0)
        visited = 0
        while queue:
            node = queue.popleft()
            visited += 1
            for target in adjacency.get(node, ()):
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
        require(visited == len(indegree), "relationship cycle rejected", "cycle", 409)
        require(conn.execute("SELECT 1 FROM tuples WHERE tenant=? AND relation='parent' "
                             "GROUP BY resource HAVING COUNT(*)>1 LIMIT 1", (tenant,)).fetchone()
                is None, "an object may have only one parent", "conflict", 409)

    def mutate(self, tenant, operations, request_id, expected_revision=None):
        identifier(tenant)
        identifier(request_id)
        require(isinstance(operations, list) and 1 <= len(operations) <= 200,
                "expected 1..200 operations")
        # Detach caller-owned mutable input and reject non-JSON values before hashing.
        try:
            ops = parse_json(canonical(operations))
        except (ValueError, TypeError, RecursionError) as exc:
            raise Problem("invalid_input", "invalid operations") from exc
        for op in ops:
            operation(op)
        if expected_revision is not None:
            integer(expected_revision)
        digest = hashlib.sha256(canonical({"ops": ops, "expected": expected_revision}).encode()).hexdigest()
        with self._tx(True) as conn:
            revision = self._revision(conn, tenant)
            replay = self._receipt(conn, tenant, "relationships", request_id, digest)
            if replay:
                return {**replay, "revision_token": self.tokens.revision(tenant, replay["revision"])}
            require(expected_revision is None or expected_revision == revision,
                    "revision compare-and-swap failed", "conflict", 409)
            for op in ops:
                action = op["op"]
                if action == "create":
                    conn.execute("INSERT OR IGNORE INTO objects VALUES(?,?)", (tenant, op["object"]))
                elif action == "drop":
                    ref = op["object"]
                    conn.execute("DELETE FROM tuples WHERE tenant=? AND subject IN (?,?)",
                                 (tenant, ref, ref + "#member"))
                    conn.execute("DELETE FROM objects WHERE tenant=? AND ref=?", (tenant, ref))
                else:
                    resource, rel, who = op["resource"], op["relation"], op["subject"]
                    self._exists(conn, tenant, resource)
                    if action == "touch":
                        if who.endswith("#member"):
                            self._exists(conn, tenant, who[:-7])
                        elif rel == "parent":
                            self._exists(conn, tenant, who)
                        conn.execute("INSERT INTO tuples VALUES(?,?,?,?,?) ON CONFLICT(tenant,resource,"
                                     "relation,subject) DO UPDATE SET expires_at=excluded.expires_at",
                                     (tenant, resource, rel, who, op.get("expires_at")))
                    else:
                        conn.execute("DELETE FROM tuples WHERE tenant=? AND resource=? AND relation=? "
                                     "AND subject=?", (tenant, resource, rel, who))
            require(conn.execute("SELECT COUNT(*) FROM objects WHERE tenant=?", (tenant,)).fetchone()[0]
                    <= 20000, "tenant object limit exceeded", "capacity", 409)
            require(conn.execute("SELECT COUNT(*) FROM tuples WHERE tenant=?", (tenant,)).fetchone()[0]
                    <= 100000, "tenant tuple limit exceeded", "capacity", 409)
            self._acyclic(conn, tenant)
            revision += 1
            conn.execute("UPDATE tenants SET revision=? WHERE id=?", (revision, tenant))
            conn.execute("INSERT INTO audit VALUES(?,?,?,?)", (tenant, revision, request_id, len(ops)))
            result = {"revision": revision, "replayed": False}
            self._save_receipt(conn, tenant, "relationships", request_id, digest, result)
        return {**result, "revision_token": self.tokens.revision(tenant, revision)}

    def _decision(self, conn, tenant, resource, perm, user, revision, now, use_cache=True):
        key = (tenant, revision, resource, perm, user)
        if use_cache:
            with self.lock:
                if now < self.last_time:
                    self.cache.clear()
                self.last_time = now
                entry = self.cache.get(key)
                if entry and entry[0] <= now < entry[1]:
                    self.cache.move_to_end(key)
                    self.hits += 1
                    return replace(entry[2], cache_hit=True, visited_edges=0)
                self.misses += 1
        evaluator = Evaluator(conn, tenant, now, self.max_edges, self.max_depth)
        decision = evaluator.evaluate(resource, perm, user, revision)
        if use_cache and self.cache_size and decision.reason != "budget_exceeded":
            # A conservative tenant-wide expiry barrier covers both positive and deny
            # dependencies, including expiring parent links and negative cache entries.
            expiry = conn.execute("SELECT MIN(expires_at) FROM tuples WHERE tenant=? AND expires_at>?",
                                  (tenant, now)).fetchone()[0]
            valid_until = min(now + self.cache_ttl, expiry) if expiry is not None else now + self.cache_ttl
            with self.lock:
                # The lower bound also rejects a late-inserted future-time decision
                # if concurrent requests observed a backwards wall-clock adjustment.
                self.cache[key] = (now, valid_until, decision)
                self.cache.move_to_end(key)
                while len(self.cache) > self.cache_size:
                    self.cache.popitem(last=False)
                    self.evictions += 1
        return decision

    @staticmethod
    def _validate_check(tenant, resource, perm, user):
        identifier(tenant)
        reference(resource)
        permission(perm)
        reference(user, ("user",))

    def check(self, tenant, resource, perm, user, min_token=None):
        self._validate_check(tenant, resource, perm, user)
        minimum = self.tokens.minimum(min_token, tenant)
        with self._tx() as conn:
            revision = self._revision(conn, tenant, minimum)  # Anchors the snapshot, even on cache hits.
            result = self._decision(conn, tenant, resource, perm, user, revision, self.clock())
        return {**result.to_dict(), "revision_token": self.tokens.revision(tenant, revision)}

    def publish(self, tenant, resource, user, text, request_id, expected_version, min_token=None):
        self._validate_check(tenant, resource, "edit", user)
        reference(resource, ("document",))
        identifier(request_id)
        integer(expected_version)
        require(isinstance(text, str), "content must be text")
        try:
            require(len(text.encode("utf-8")) <= 32768, "content exceeds 32 KiB")
        except UnicodeError as exc:
            raise Problem("invalid_input", "invalid text encoding") from exc
        minimum = self.tokens.minimum(min_token, tenant)
        digest = hashlib.sha256(canonical({"resource": resource, "user": user, "text": text,
                                           "expected": expected_version}).encode()).hexdigest()
        with self._tx(True) as conn:
            revision = self._revision(conn, tenant, minimum)
            decision = self._decision(conn, tenant, resource, "edit", user, revision, self.clock(), False)
            require(decision.allowed, "edit not permitted", "forbidden", 403)
            # A receipt is not a permanent authorization grant: recheck on every retry.
            replay = self._receipt(conn, tenant, "publish", request_id, digest)
            if replay:
                return {**replay, "revision_token": self.tokens.revision(tenant, replay["acl_revision"])}
            row = conn.execute("SELECT version FROM contents WHERE tenant=? AND resource=?",
                               (tenant, resource)).fetchone()
            version = row[0] if row else 0
            require(version == expected_version, "content version compare-and-swap failed", "conflict", 409)
            version += 1
            conn.execute("INSERT INTO contents VALUES(?,?,?,?,?) ON CONFLICT(tenant,resource) "
                         "DO UPDATE SET body=excluded.body,version=excluded.version,"
                         "acl_revision=excluded.acl_revision", (tenant, resource, text, version, revision))
            result = {"resource": resource, "version": version, "acl_revision": revision, "replayed": False}
            self._save_receipt(conn, tenant, "publish", request_id, digest, result)
        return {**result, "revision_token": self.tokens.revision(tenant, revision)}

    def read_content(self, tenant, resource, user, min_token=None, *, _after_authorize=None):
        self._validate_check(tenant, resource, "view", user)
        reference(resource, ("document",))
        minimum = self.tokens.minimum(min_token, tenant)
        with self._tx() as conn:
            revision = self._revision(conn, tenant, minimum)
            decision = self._decision(conn, tenant, resource, "view", user, revision, self.clock())
            require(decision.allowed, "view not permitted", "forbidden", 403)
            if _after_authorize:
                _after_authorize()  # Test-only race barrier; never exposed through HTTP.
            row = conn.execute("SELECT body,version,acl_revision FROM contents WHERE tenant=? AND resource=?",
                               (tenant, resource)).fetchone()
            require(row is not None, "document content not found", "not_found", 404)
            require(row["acl_revision"] <= revision, "content snapshot mismatch", "integrity", 500)
            return {"resource": resource, "body": row["body"], "version": row["version"],
                    "content_acl_revision": row["acl_revision"], "checked_revision": revision,
                    "revision_token": self.tokens.revision(tenant, revision)}

    def stats(self):
        with self.lock:
            return {"hits": self.hits, "misses": self.misses, "evictions": self.evictions,
                    "entries": len(self.cache)}
