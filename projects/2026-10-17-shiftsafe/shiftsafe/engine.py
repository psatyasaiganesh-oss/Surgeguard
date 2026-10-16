"""SQLite implementation of dual writes, fenced backfill, and gated cutover.

All cooperating writers use this Engine. Both representations deliberately share
one transactional database. This is not a cross-database dual-write protocol.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import sqlite3
import time

from .errors import (Busy, Conflict, ConsistencyError, InvalidTransition, LeaseBusy,
                     LeaseLost, NotFound, StaleBatch, ValidationError)
from .model import Batch, Snapshot, canonical, digest, identifier, integer, subscriptions


SCHEMA = """
CREATE TABLE IF NOT EXISTS migration (
 id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN ('LEGACY','DUAL','SHADOW','NEW','FINALIZED')),
 cursor INTEGER NOT NULL DEFAULT 0, watermark INTEGER NOT NULL DEFAULT 0,
 data_version INTEGER NOT NULL DEFAULT 0, owner TEXT, fence INTEGER NOT NULL DEFAULT 0,
 expires REAL NOT NULL DEFAULT 0, shadow_reads INTEGER NOT NULL DEFAULT 0,
 shadow_mismatches INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO migration(id,schema_version,phase) VALUES(1,1,'LEGACY');
CREATE TABLE IF NOT EXISTS customers (
 customer_id INTEGER PRIMARY KEY CHECK(customer_id>0),
 revision INTEGER NOT NULL CHECK(revision>0), deleted INTEGER NOT NULL CHECK(deleted IN (0,1))
);
CREATE TABLE IF NOT EXISTS legacy_customers (
 customer_id INTEGER PRIMARY KEY REFERENCES customers(customer_id),
 revision INTEGER NOT NULL, deleted INTEGER NOT NULL CHECK(deleted IN (0,1)),
 payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_state (
 customer_id INTEGER PRIMARY KEY REFERENCES customers(customer_id),
 revision INTEGER NOT NULL, deleted INTEGER NOT NULL CHECK(deleted IN (0,1))
);
CREATE TABLE IF NOT EXISTS subscriptions_v2 (
 customer_id INTEGER NOT NULL REFERENCES projection_state(customer_id) ON DELETE CASCADE,
 subscription_id TEXT NOT NULL, plan TEXT NOT NULL,
 amount_cents INTEGER NOT NULL CHECK(amount_cents>=0), currency TEXT NOT NULL,
 status TEXT NOT NULL, PRIMARY KEY(customer_id,subscription_id)
);
CREATE TABLE IF NOT EXISTS operations (
 operation_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, response TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, data TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class Lease:
    owner: str
    fence: int


class Engine:
    def __init__(self, path, *, clock=time.time, timeout=5.0, fault=None):
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 <= timeout <= 60:
            raise ValidationError("timeout must be finite and between 0 and 60 seconds")
        self.path = str(Path(path).resolve())
        self.clock, self.timeout, self.fault = clock, timeout, fault
        # Reject symlinks rather than opening an unexpected operator-owned file.
        if Path(path).is_symlink():
            raise ValidationError("database path cannot be a symlink")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600) if not Path(self.path).exists() else None
        if fd is not None:
            os.close(fd)
        con = self._connect()
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript(SCHEMA)
            if self._meta(con)["schema_version"] != 1:
                raise ValidationError("unsupported database schema version")
        except sqlite3.OperationalError as exc:
            self._translate(exc)
        finally:
            con.close()

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA synchronous=FULL")
        return con

    @staticmethod
    def _translate(exc):
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            raise Busy("database busy; retry the entire operation with the same operation_id") from exc
        raise exc

    @contextmanager
    def _tx(self, write=False):
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield con
            con.commit()
        except sqlite3.OperationalError as exc:
            con.rollback()
            self._translate(exc)
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    @staticmethod
    def _meta(con):
        return dict(con.execute("SELECT * FROM migration WHERE id=1").fetchone())

    @staticmethod
    def _event(con, kind, data):
        con.execute("INSERT INTO events(kind,data) VALUES(?,?)", (kind, canonical(data)))

    def _point(self, name):
        if self.fault:
            self.fault(name)

    @staticmethod
    def _phase(meta, allowed):
        if meta["phase"] not in allowed:
            raise InvalidTransition(f"operation requires {sorted(allowed)}; current phase is {meta['phase']}")

    @staticmethod
    def _legacy(con, customer_id):
        row = con.execute("SELECT * FROM legacy_customers WHERE customer_id=?", (customer_id,)).fetchone()
        return Snapshot(row["customer_id"], row["revision"], bool(row["deleted"]), row["payload"]) if row else None

    @staticmethod
    def _normalized(con, customer_id):
        state = con.execute("SELECT * FROM projection_state WHERE customer_id=?", (customer_id,)).fetchone()
        if state is None:
            return None
        rows = con.execute("""SELECT subscription_id AS id,plan,amount_cents,currency,status
                              FROM subscriptions_v2 WHERE customer_id=? ORDER BY subscription_id""", (customer_id,))
        return {"customer_id": customer_id, "revision": state["revision"],
                "deleted": bool(state["deleted"]), "subscriptions": [dict(r) for r in rows]}

    @staticmethod
    def _project(con, snapshot, *, force=False):
        row = con.execute("SELECT revision FROM projection_state WHERE customer_id=?", (snapshot.customer_id,)).fetchone()
        if row is not None and row["revision"] >= snapshot.revision and not force:
            return False
        con.execute("""INSERT INTO projection_state VALUES(?,?,?) ON CONFLICT(customer_id)
                       DO UPDATE SET revision=excluded.revision,deleted=excluded.deleted""",
                    (snapshot.customer_id, snapshot.revision, int(snapshot.deleted)))
        con.execute("DELETE FROM subscriptions_v2 WHERE customer_id=?", (snapshot.customer_id,))
        con.executemany("INSERT INTO subscriptions_v2 VALUES(?,?,?,?,?,?)", [
            (snapshot.customer_id, s["id"], s["plan"], s["amount_cents"], s["currency"], s["status"])
            for s in json.loads(snapshot.payload)
        ])
        return True

    def put(self, customer_id, items, *, operation_id, expected_revision=None):
        return self._write(customer_id, subscriptions(items), False, operation_id, expected_revision)

    def delete(self, customer_id, *, operation_id, expected_revision=None):
        return self._write(customer_id, [], True, operation_id, expected_revision)

    def _write(self, customer_id, items, deleted, operation_id, expected_revision):
        integer(customer_id, "customer_id", 1)
        identifier(operation_id)
        if expected_revision is not None:
            integer(expected_revision, "expected_revision")
        request_hash = digest([customer_id, items, deleted, expected_revision])
        with self._tx(write=True) as con:
            receipt = con.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if receipt:
                if receipt["request_hash"] != request_hash:
                    raise Conflict("operation_id reused for a different request")
                return {**json.loads(receipt["response"]), "replayed": True}
            meta = self._meta(con)
            existing = con.execute("SELECT * FROM customers WHERE customer_id=?", (customer_id,)).fetchone()
            revision = existing["revision"] if existing else 0
            if expected_revision is not None and expected_revision != revision:
                raise Conflict(f"expected revision {expected_revision}; actual revision {revision}")
            if deleted and existing is None:
                raise NotFound("customer does not exist")
            snapshot = Snapshot(customer_id, revision + 1, deleted, canonical(items))
            con.execute("""INSERT INTO customers VALUES(?,?,?) ON CONFLICT(customer_id)
                           DO UPDATE SET revision=excluded.revision,deleted=excluded.deleted""",
                        (customer_id, snapshot.revision, int(deleted)))
            # NEW reverses write order; the whole transaction still commits atomically.
            if meta["phase"] in ("NEW", "FINALIZED"):
                self._project(con, snapshot, force=True)
                self._point("after_projection_write")
            if meta["phase"] != "FINALIZED":
                con.execute("""INSERT INTO legacy_customers VALUES(?,?,?,?) ON CONFLICT(customer_id)
                               DO UPDATE SET revision=excluded.revision,deleted=excluded.deleted,payload=excluded.payload""",
                            (customer_id, snapshot.revision, int(deleted), snapshot.payload))
                self._point("after_legacy_write")
            if meta["phase"] in ("DUAL", "SHADOW"):
                self._project(con, snapshot, force=True)
                self._point("after_projection_write")
            response = {"customer_id": customer_id, "revision": snapshot.revision, "deleted": deleted}
            con.execute("INSERT INTO operations VALUES(?,?,?)", (operation_id, request_hash, canonical(response)))
            con.execute("UPDATE migration SET data_version=data_version+1 WHERE id=1")
            self._point("before_write_commit")
            return {**response, "replayed": False}

    def get(self, customer_id):
        integer(customer_id, "customer_id", 1)
        match = None
        with self._tx() as con:
            phase = self._meta(con)["phase"]
            legacy = self._legacy(con, customer_id) if phase != "FINALIZED" else None
            normalized = self._normalized(con, customer_id) if phase != "LEGACY" else None
            old = legacy.record() if legacy else None
            if phase in ("SHADOW", "NEW"):
                match = old == normalized
            result = normalized if phase in ("NEW", "FINALIZED") else old
            if result is None:
                raise NotFound("customer does not exist in active read store")
            result = {**result, "source": "new" if phase in ("NEW", "FINALIZED") else "legacy", "shadow_match": match}
        # Metrics are historical observations, not a cutover authorization token.
        if match is not None:
            with self._tx(write=True) as con:
                con.execute("""UPDATE migration SET shadow_reads=shadow_reads+1,
                               shadow_mismatches=shadow_mismatches+? WHERE id=1""", (int(not match),))
        return result

    def start(self):
        with self._tx(write=True) as con:
            meta = self._meta(con)
            self._phase(meta, {"LEGACY"})
            watermark = con.execute("SELECT COALESCE(MAX(customer_id),0) FROM customers").fetchone()[0]
            con.execute("UPDATE migration SET phase='DUAL',watermark=? WHERE id=1", (watermark,))
            self._event(con, "migration_started", {"watermark": watermark})
        return self.status()

    @staticmethod
    def _ttl(ttl):
        if type(ttl) not in (int, float) or not math.isfinite(ttl) or not 0 < ttl <= 300:
            raise ValidationError("lease ttl must be finite and in (0,300] seconds")

    def _lease(self, con, lease):
        if not isinstance(lease, Lease):
            raise LeaseLost("invalid lease")
        meta = self._meta(con)
        if meta["owner"] != lease.owner or meta["fence"] != lease.fence or meta["expires"] <= self.clock():
            raise LeaseLost("worker lease expired or was fenced out")
        return meta

    def acquire(self, owner, *, ttl=30):
        identifier(owner, "owner")
        self._ttl(ttl)
        with self._tx(write=True) as con:
            meta = self._meta(con)
            self._phase(meta, {"DUAL"})
            if meta["owner"] is not None and meta["expires"] > self.clock():
                raise LeaseBusy("a backfill worker already holds the lease")
            token = meta["fence"] + 1
            con.execute("UPDATE migration SET owner=?,fence=?,expires=? WHERE id=1", (owner, token, self.clock() + ttl))
            self._event(con, "lease_acquired", {"owner": owner, "fence": token})
        return Lease(owner, token)

    def renew(self, lease, *, ttl=30):
        self._ttl(ttl)
        with self._tx(write=True) as con:
            self._lease(con, lease)
            con.execute("UPDATE migration SET expires=? WHERE id=1", (self.clock() + ttl,))

    def release(self, lease):
        with self._tx(write=True) as con:
            self._lease(con, lease)
            con.execute("UPDATE migration SET owner=NULL,expires=0 WHERE id=1")

    def capture(self, lease, *, batch_size=100):
        integer(batch_size, "batch_size", 1, 1000)
        with self._tx() as con:
            meta = self._lease(con, lease)
            self._phase(meta, {"DUAL"})
            rows = con.execute("""SELECT * FROM legacy_customers WHERE customer_id>? AND customer_id<=?
                                  ORDER BY customer_id LIMIT ?""", (meta["cursor"], meta["watermark"], batch_size)).fetchall()
            snapshots = tuple(Snapshot(r["customer_id"], r["revision"], bool(r["deleted"]), r["payload"]) for r in rows)
            cursor_after = rows[-1]["customer_id"] if len(rows) == batch_size else meta["watermark"]
            return Batch(lease.owner, lease.fence, meta["cursor"], cursor_after, meta["watermark"], snapshots)

    def apply(self, lease, batch):
        if not isinstance(batch, Batch) or (batch.owner, batch.fence) != (lease.owner, lease.fence):
            raise StaleBatch("batch belongs to a different lease")
        with self._tx(write=True) as con:
            meta = self._lease(con, lease)
            self._phase(meta, {"DUAL"})
            if meta["cursor"] != batch.cursor_before or meta["watermark"] != batch.watermark:
                raise StaleBatch("checkpoint changed; capture a new batch")
            applied = sum(self._project(con, row) for row in batch.rows)
            self._point("after_batch_projection")
            con.execute("UPDATE migration SET cursor=? WHERE id=1", (batch.cursor_after,))
            self._point("after_batch_checkpoint")
            self._event(con, "batch_committed", {"cursor": batch.cursor_after, "scanned": len(batch.rows), "applied": applied})
            return {"scanned": len(batch.rows), "applied": applied, "skipped_newer_or_equal": len(batch.rows)-applied,
                    "cursor": batch.cursor_after, "done": batch.cursor_after >= meta["watermark"]}

    def backfill(self, lease, *, batch_size=100):
        return self.apply(lease, self.capture(lease, batch_size=batch_size))

    def _verify(self, con):
        # A bounded local lab: full snapshot comparison. See README for scaling limits.
        ids = [r[0] for r in con.execute("""SELECT customer_id FROM customers UNION SELECT customer_id
                                            FROM legacy_customers UNION SELECT customer_id FROM projection_state
                                            ORDER BY customer_id""")]
        mismatch_ids, source_hashes, target_hashes = [], [], []
        for cid in ids:
            legacy = self._legacy(con, cid)
            source = legacy.record() if legacy else None
            target = self._normalized(con, cid)
            entity = con.execute("SELECT revision,deleted FROM customers WHERE customer_id=?", (cid,)).fetchone()
            metadata_ok = source is not None and entity is not None and (source["revision"], int(source["deleted"])) == tuple(entity)
            if source != target or not metadata_ok:
                mismatch_ids.append(cid)
            source_hashes.append([cid, digest(source)])
            target_hashes.append([cid, digest(target)])
        return {"ok": not mismatch_ids, "customers_checked": len(ids), "mismatches": len(mismatch_ids),
                "example_customer_ids": mismatch_ids[:20], "source_digest": digest(source_hashes),
                "target_digest": digest(target_hashes), "data_version": self._meta(con)["data_version"]}

    def verify(self):
        with self._tx() as con:
            self._phase(self._meta(con), {"DUAL", "SHADOW", "NEW"})
            return self._verify(con)

    def repair(self, customer_id):
        integer(customer_id, "customer_id", 1)
        with self._tx(write=True) as con:
            self._phase(self._meta(con), {"DUAL", "SHADOW", "NEW"})
            source = self._legacy(con, customer_id)
            if source is None:
                raise NotFound("no legacy source exists for repair")
            self._project(con, source, force=True)
            self._event(con, "customer_repaired", {"customer_id": customer_id, "revision": source.revision})

    def shadow(self):
        with self._tx(write=True) as con:
            meta = self._meta(con)
            self._phase(meta, {"DUAL"})
            if meta["cursor"] < meta["watermark"]:
                raise InvalidTransition("backfill must finish before shadow reads")
            con.execute("UPDATE migration SET phase='SHADOW',owner=NULL,expires=0 WHERE id=1")
            self._event(con, "shadow_enabled", {})
        return self.status()

    def _gated_transition(self, before, after):
        with self._tx(write=True) as con:
            self._phase(self._meta(con), {before})
            report = self._verify(con)
            if not report["ok"]:
                raise ConsistencyError(report)
            self._point("before_gate_commit")
            con.execute("UPDATE migration SET phase=? WHERE id=1", (after,))
            self._event(con, "phase_changed", {"from": before, "to": after, "verified_data_version": report["data_version"],
                                                "digest": report["source_digest"]})
        return report

    def cutover(self):
        return self._gated_transition("SHADOW", "NEW")

    def rollback(self):
        with self._tx(write=True) as con:
            meta = self._meta(con)
            self._phase(meta, {"SHADOW", "NEW"})
            con.execute("UPDATE migration SET phase='DUAL',owner=NULL,expires=0 WHERE id=1")
            self._event(con, "rolled_back", {"from": meta["phase"]})
        return self.status()

    def finalize(self):
        return self._gated_transition("NEW", "FINALIZED")

    def cleanup(self, *, batch_size=100):
        integer(batch_size, "batch_size", 1, 1000)
        with self._tx(write=True) as con:
            self._phase(self._meta(con), {"FINALIZED"})
            cur = con.execute("""DELETE FROM legacy_customers WHERE customer_id IN
                                 (SELECT customer_id FROM legacy_customers ORDER BY customer_id LIMIT ?)""", (batch_size,))
            self._event(con, "legacy_cleaned", {"removed": cur.rowcount})
            return cur.rowcount

    def status(self):
        with self._tx() as con:
            meta = self._meta(con)
            result = {k: meta[k] for k in ("phase", "cursor", "watermark", "data_version", "fence", "shadow_reads", "shadow_mismatches")}
            result["backfill_complete"] = meta["phase"] != "LEGACY" and meta["cursor"] >= meta["watermark"]
            for table in ("customers", "legacy_customers", "projection_state", "subscriptions_v2", "operations"):
                result[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            return result

    def events(self, *, after=0, limit=100):
        integer(after, "after")
        integer(limit, "limit", 1, 1000)
        with self._tx() as con:
            return [{"seq": r["seq"], "kind": r["kind"], "data": json.loads(r["data"])}
                    for r in con.execute("SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?", (after, limit))]
