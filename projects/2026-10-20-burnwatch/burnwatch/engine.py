"""Immutable minute reports, completeness watermarks, and transactional alert state."""

import hashlib
import os
import secrets
import sqlite3
import stat
import time
from contextlib import contextmanager
from pathlib import Path

from .model import (MINUTE, Problem, assess, bucket, canonical, identifier, integer,
                    minute, parse_json, require, validate_policy)

APPLICATION_ID = 0x42575231
SCHEMA = (
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE services(name TEXT PRIMARY KEY, policy TEXT NOT NULL, start INTEGER NOT NULL, "
    "revision INTEGER NOT NULL DEFAULT 0, severity TEXT NOT NULL DEFAULT 'CLEAR', "
    "quality TEXT NOT NULL DEFAULT 'WARMING', holding INTEGER NOT NULL DEFAULT 0, "
    "active TEXT NOT NULL DEFAULT '{}', seq INTEGER NOT NULL DEFAULT 0, result TEXT)",
    "CREATE TABLE producers(service TEXT NOT NULL, id TEXT NOT NULL, watermark INTEGER NOT NULL, "
    "PRIMARY KEY(service,id), FOREIGN KEY(service) REFERENCES services(name))",
    "CREATE TABLE samples(service TEXT NOT NULL, producer TEXT NOT NULL, minute INTEGER NOT NULL, "
    "total INTEGER NOT NULL CHECK(total>=0), bad INTEGER NOT NULL CHECK(bad>=0 AND bad<=total), "
    "PRIMARY KEY(service,producer,minute), FOREIGN KEY(service,producer) REFERENCES producers(service,id))",
    "CREATE TABLE rollups(service TEXT NOT NULL, minute INTEGER NOT NULL, total INTEGER NOT NULL CHECK(total>=0), "
    "bad INTEGER NOT NULL CHECK(bad>=0 AND bad<=total), contributors INTEGER NOT NULL CHECK(contributors>0), "
    "PRIMARY KEY(service,minute), FOREIGN KEY(service) REFERENCES services(name))",
    "CREATE TABLE receipts(service TEXT NOT NULL, producer TEXT NOT NULL, batch_id TEXT NOT NULL, "
    "digest TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(service,producer,batch_id), "
    "FOREIGN KEY(service,producer) REFERENCES producers(service,id))",
    "CREATE TABLE api_keys(digest TEXT PRIMARY KEY, service TEXT NOT NULL, producer TEXT NOT NULL, "
    "expires REAL NOT NULL, FOREIGN KEY(service,producer) REFERENCES producers(service,id))",
    "CREATE TABLE outbox(event_id TEXT PRIMARY KEY, service TEXT NOT NULL, seq INTEGER NOT NULL, "
    "payload TEXT NOT NULL, delivered REAL, UNIQUE(service,seq), FOREIGN KEY(service) REFERENCES services(name))",
)


def detached(value):
    try:
        return parse_json(canonical(value))
    except (TypeError, ValueError, RecursionError) as exc:
        raise Problem("invalid_input", "expected bounded JSON data") from exc


class Engine:
    def __init__(self, path, *, clock=time.time):
        require(str(path) != ":memory:", "use a persistent file-backed database")
        self.path, self.clock = str(Path(path).absolute()), clock
        self._initialize()

    def _initialize(self):
        created = False
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            created = True
        except FileExistsError:
            info = os.lstat(self.path)
            require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) & 0o077 == 0,
                    "database must be a private regular non-symlink file (0600)")
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        try:
            if created:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                for statement in SCHEMA:
                    conn.execute(statement)
                conn.execute("INSERT INTO meta VALUES('database_id',?)", (secrets.token_hex(16),))
                conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
                conn.execute("PRAGMA user_version=1")
                conn.commit()
            else:
                require(conn.execute("PRAGMA application_id").fetchone()[0] == APPLICATION_ID
                        and conn.execute("PRAGMA user_version").fetchone()[0] == 1
                        and conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal",
                        "not a supported BurnWatch database")
            self.database_id = conn.execute("SELECT value FROM meta WHERE key='database_id'").fetchone()[0]
        finally:
            conn.close()

    @contextmanager
    def tx(self, write=False):
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
                raise Problem("busy", "database busy; retry the same batch ID", 503) from exc
            raise
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def service(conn, name):
        row = conn.execute("SELECT * FROM services WHERE name=?", (name,)).fetchone()
        require(row is not None, "service not found", "not_found", 404)
        return row

    def register(self, service, producers, policy, start_minute):
        identifier(service)
        require(isinstance(producers, list) and 1 <= len(producers) <= 8, "expected 1..8 producers")
        for producer in producers:
            identifier(producer)
        require(len(set(producers)) == len(producers), "duplicate producer")
        policy = validate_policy(detached(policy))
        minute(start_minute)
        require(start_minute <= self.clock() // MINUTE * MINUTE, "start is in the future")
        with self.tx(True) as conn:
            old = conn.execute("SELECT * FROM services WHERE name=?", (service,)).fetchone()
            if old:
                existing = [row[0] for row in conn.execute("SELECT id FROM producers WHERE service=? ORDER BY id", (service,))]
                require(old["policy"] == canonical(policy) and old["start"] == start_minute
                        and existing == sorted(producers), "registration conflicts with existing service", "conflict", 409)
                return {"service": service, "revision": old["revision"], "replayed": True}
            require(conn.execute("SELECT COUNT(*) FROM services").fetchone()[0] < 128,
                    "service capacity exceeded", "capacity", 409)
            conn.execute("INSERT INTO services(name,policy,start) VALUES(?,?,?)", (service, canonical(policy), start_minute))
            conn.executemany("INSERT INTO producers VALUES(?,?,?)", [(service, p, start_minute) for p in producers])
        return {"service": service, "revision": 0, "replayed": False}

    def ingest(self, service, producer, batch_id, buckets, watermark=None):
        identifier(service)
        identifier(producer)
        identifier(batch_id)
        values = detached(buckets)
        require(isinstance(values, list) and len(values) <= 500, "expected at most 500 buckets")
        for value in values:
            bucket(value)
        require(len({v["minute"] for v in values}) == len(values), "duplicate minute within batch")
        if watermark is not None:
            minute(watermark)
        digest = hashlib.sha256(canonical({"buckets": values, "watermark": watermark}).encode()).hexdigest()
        now = self.clock() // MINUTE * MINUTE
        with self.tx(True) as conn:
            state = self.service(conn, service)
            source = conn.execute("SELECT watermark FROM producers WHERE service=? AND id=?", (service, producer)).fetchone()
            require(source is not None, "producer not registered", "not_found", 404)
            receipt = conn.execute("SELECT digest,result FROM receipts WHERE service=? AND producer=? AND batch_id=?",
                                   (service, producer, batch_id)).fetchone()
            if receipt:
                require(receipt["digest"] == digest, "batch ID reused with different input", "conflict", 409)
                return {**parse_json(receipt["result"]), "replayed": True}
            old = source[0]
            new = old if watermark is None else watermark
            require(old <= new <= now, "watermark must not regress or enter the future", "watermark", 409)
            require(new - old <= 500 * MINUTE, "watermark may advance by at most 500 minutes per batch")
            accepted = duplicates = 0
            for value in values:
                timestamp, total, bad = value["minute"], value["total"], value["bad"]
                require(timestamp >= state["start"] and timestamp + MINUTE <= now,
                        "only closed minute buckets within service history are accepted")
                existing = conn.execute("SELECT total,bad FROM samples WHERE service=? AND producer=? AND minute=?",
                                        (service, producer, timestamp)).fetchone()
                if existing:
                    require(tuple(existing) == (total, bad), "minute bucket is immutable", "conflict", 409)
                    duplicates += 1
                    continue
                require(timestamp >= old, "sealed or pruned minute cannot be inserted", "sealed", 409)
                require(timestamp < old + 7200 * MINUTE, "pending buffer exceeds 7200 minutes", "capacity", 409)
                conn.execute("INSERT INTO samples VALUES(?,?,?,?,?)", (service, producer, timestamp, total, bad))
                conn.execute("INSERT INTO rollups VALUES(?,?,?,?,1) ON CONFLICT(service,minute) DO UPDATE SET "
                             "total=rollups.total+excluded.total,bad=rollups.bad+excluded.bad,"
                             "contributors=rollups.contributors+1", (service, timestamp, total, bad))
                accepted += 1
            if new > old:
                count = conn.execute("SELECT COUNT(*) FROM samples WHERE service=? AND producer=? AND minute>=? AND minute<?",
                                     (service, producer, old, new)).fetchone()[0]
                require(count == (new - old) // MINUTE, "watermark would seal a telemetry gap", "gap", 409)
                conn.execute("UPDATE producers SET watermark=? WHERE service=? AND id=?", (new, service, producer))
            revision = state["revision"] + int(bool(accepted or new != old))
            conn.execute("UPDATE services SET revision=? WHERE name=?", (revision, service))
            result = {"accepted": accepted, "duplicates": duplicates, "producer_watermark": new,
                      "revision": revision, "replayed": False}
            conn.execute("INSERT INTO receipts VALUES(?,?,?,?,?)", (service, producer, batch_id, digest, canonical(result)))
        return result

    def mint(self, service, producer, ttl=3600):
        identifier(service)
        identifier(producer)
        integer(ttl, 1, 86400)
        token = "bw_" + secrets.token_hex(32)
        digest = hashlib.sha256(token.encode()).hexdigest()
        with self.tx(True) as conn:
            require(conn.execute("SELECT 1 FROM producers WHERE service=? AND id=?", (service, producer)).fetchone(),
                    "producer not found", "not_found", 404)
            conn.execute("INSERT INTO api_keys VALUES(?,?,?,?)", (digest, service, producer, self.clock() + ttl))
        return token

    def authenticate(self, token):
        require(isinstance(token, str) and len(token) == 67 and token.startswith("bw_"),
                "invalid credential", "unauthorized", 401)
        digest = hashlib.sha256(token.encode()).hexdigest()
        with self.tx() as conn:
            row = conn.execute("SELECT service,producer,expires FROM api_keys WHERE digest=?", (digest,)).fetchone()
            require(row is not None and self.clock() < row["expires"], "invalid or expired credential", "unauthorized", 401)
            return {"service": row["service"], "producer": row["producer"]}

    def revoke(self, token):
        require(isinstance(token, str) and len(token) <= 128, "invalid credential")
        with self.tx(True) as conn:
            conn.execute("DELETE FROM api_keys WHERE digest=?", (hashlib.sha256(token.encode()).hexdigest(),))

    @staticmethod
    def _producers(conn, service):
        return [dict(row) for row in conn.execute("SELECT id,watermark FROM producers WHERE service=? ORDER BY id", (service,))]

    def evaluate(self, service):
        identifier(service)
        with self.tx(True) as conn:
            state = self.service(conn, service)
            policy = parse_json(state["policy"])
            producers = self._producers(conn, service)
            end = min(p["watermark"] for p in producers)
            now = self.clock()
            require(end <= now, "server clock precedes sealed telemetry", "clock", 409)
            stale = now - end > policy["stale_after_seconds"]
            lengths = {rule[key] for rule in policy["rules"] for key in ("long_minutes", "short_minutes")}
            windows = {}
            for length in lengths:
                row = conn.execute("SELECT COALESCE(SUM(total),0),COALESCE(SUM(bad),0),COUNT(*),"
                                   "COALESCE(SUM(CASE WHEN contributors=? THEN 1 ELSE 0 END),0) "
                                   "FROM rollups WHERE service=? AND minute>=? AND minute<?",
                                   (len(producers), service, end - length * MINUTE, end)).fetchone()
                windows[length] = dict(zip(("total", "bad", "minutes", "complete_minutes"), row))
            decision = assess(policy, state["start"], end, windows, parse_json(state["active"]), stale)
            old = (state["severity"], state["quality"], bool(state["holding"]))
            new = (decision["severity"], decision["quality"], decision["holding"])
            seq = state["seq"] + int(old != new)
            result = {"service": service, "revision": state["revision"], "end_minute": end,
                      "evaluated_at": now, "seq": seq, "target_ppm": policy["target_ppm"],
                      "producers": [{**p, "lag_seconds": max(0, now - p["watermark"])} for p in producers], **decision}
            if old != new:
                event_id = hashlib.sha256(f"{self.database_id}:{service}:{seq}".encode()).hexdigest()
                payload = {"event_id": event_id, "service": service, "seq": seq, "end_minute": end,
                           "kind": "severity" if old[0] != new[0] else "telemetry",
                           "from": {"severity": old[0], "quality": old[1], "holding": old[2]},
                           "to": {"severity": new[0], "quality": new[1], "holding": new[2]},
                           "status": decision["status"]}
                conn.execute("INSERT INTO outbox VALUES(?,?,?,?,NULL)", (event_id, service, seq, canonical(payload)))
            conn.execute("UPDATE services SET severity=?,quality=?,holding=?,active=?,seq=?,result=? WHERE name=?",
                         (*new, canonical(decision["active"]), seq, canonical(result), service))
        return result

    def snapshot(self, service):
        identifier(service)
        with self.tx() as conn:
            state = self.service(conn, service)
            producers = self._producers(conn, service)
            end = min(p["watermark"] for p in producers)
            return {"service": service, "revision": state["revision"], "end_minute": end,
                    "current_lag_seconds": max(0, self.clock() - end),
                    "last_evaluation": parse_json(state["result"]) if state["result"] else None}

    def pending(self, limit=100):
        integer(limit, 1, 100)
        with self.tx() as conn:
            return [parse_json(row[0]) for row in conn.execute(
                "SELECT payload FROM outbox WHERE delivered IS NULL ORDER BY service,seq LIMIT ?", (limit,))]

    def acknowledge(self, event_id):
        require(isinstance(event_id, str) and len(event_id) == 64, "invalid event ID")
        with self.tx(True) as conn:
            require(conn.execute("SELECT 1 FROM outbox WHERE event_id=?", (event_id,)).fetchone(),
                    "notification not found", "not_found", 404)
            conn.execute("UPDATE outbox SET delivered=COALESCE(delivered,?) WHERE event_id=?", (self.clock(), event_id))

    def prune(self, service):
        identifier(service)
        with self.tx(True) as conn:
            state = self.service(conn, service)
            policy = parse_json(state["policy"])
            end = min(p["watermark"] for p in self._producers(conn, service))
            cutoff = end - max(rule["long_minutes"] for rule in policy["rules"]) * MINUTE
            raw = conn.execute("DELETE FROM samples WHERE service=? AND minute<?", (service, cutoff)).rowcount
            rollups = conn.execute("DELETE FROM rollups WHERE service=? AND minute<?", (service, cutoff)).rowcount
        return {"raw_deleted": raw, "rollups_deleted": rollups, "before_minute": cutoff}

    def verify(self, service):
        identifier(service)
        with self.tx() as conn:
            self.service(conn, service)
            raw = {r[0]: tuple(r)[1:] for r in conn.execute("SELECT minute,SUM(total),SUM(bad),COUNT(*) "
                    "FROM samples WHERE service=? GROUP BY minute", (service,))}
            materialized = {r[0]: tuple(r)[1:] for r in conn.execute(
                "SELECT minute,total,bad,contributors FROM rollups WHERE service=?", (service,))}
            mismatches = sum(raw.get(key) != materialized.get(key) for key in raw.keys() | materialized.keys())
            return {"service": service, "minutes": len(raw), "mismatches": mismatches, "passed": mismatches == 0}

    def metrics(self, service):
        snapshot = self.snapshot(service)
        result = snapshot["last_evaluation"]
        label = f'service="{service}"'
        lines = ["# TYPE burnwatch_watermark_lag_seconds gauge",
                 f'burnwatch_watermark_lag_seconds{{{label}}} {snapshot["current_lag_seconds"]}',
                 "# TYPE burnwatch_evaluation_stale gauge"]
        policy_stale = 0
        with self.tx() as conn:
            policy_stale = parse_json(self.service(conn, service)["policy"])["stale_after_seconds"]
        evaluation_stale = (not result or self.clock() - result["evaluated_at"] > policy_stale
                            or result["end_minute"] != snapshot["end_minute"])
        lines.append(f'burnwatch_evaluation_stale{{{label}}} {int(evaluation_stale)}')
        fresh = snapshot["current_lag_seconds"] <= policy_stale
        lines.extend(["# TYPE burnwatch_telemetry_fresh gauge",
                      f'burnwatch_telemetry_fresh{{{label}}} {int(fresh)}',
                      "# TYPE burnwatch_evidence_complete gauge",
                      f'burnwatch_evidence_complete{{{label}}} ' + str(int(bool(
                          result and result["quality"] == "COMPLETE" and fresh and not evaluation_stale)))])
        if result:
            lines.extend(["# TYPE burnwatch_alert_severity gauge",
                          f'burnwatch_alert_severity{{{label}}} ' + str({"CLEAR": 0, "TICKET": 1, "PAGE": 2}[result["severity"]])])
            for rule in result["rules"]:
                for window in ("long", "short"):
                    value = rule[window]["burn_rate"]
                    if value is not None:
                        lines.append(f'burnwatch_burn_rate{{{label},rule="{rule["name"]}",window="{window}"}} {value}')
        return "\n".join(lines) + "\n"
