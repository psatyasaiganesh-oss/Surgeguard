"""At-least-once local delivery with a durable, idempotent demonstration inbox."""

import hashlib
import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path

from .model import canonical, fields, parse_json, require

INBOX_ID = 0x42574931


class Inbox:
    def __init__(self, path):
        self.path = str(Path(path).absolute())
        created = False
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            created = True
        except FileExistsError:
            info = os.lstat(self.path)
            require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) & 0o077 == 0,
                    "inbox must be a private regular non-symlink file")
        with self.connection() as conn:
            if created:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("CREATE TABLE inbox(event_id TEXT PRIMARY KEY, digest TEXT NOT NULL, payload TEXT NOT NULL)")
                conn.execute(f"PRAGMA application_id={INBOX_ID}")
            else:
                require(conn.execute("PRAGMA application_id").fetchone()[0] == INBOX_ID,
                        "not a BurnWatch inbox")

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def store(self, event):
        fields(event, ("event_id", "service", "seq", "end_minute", "kind", "from", "to", "status"))
        payload = canonical(event)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self.connection() as conn:
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT digest FROM inbox WHERE event_id=?", (event["event_id"],)).fetchone()
            if existing:
                require(existing[0] == digest, "event ID conflicts with inbox payload", "integrity", 409)
                return False
            conn.execute("INSERT INTO inbox VALUES(?,?,?)", (event["event_id"], digest, payload))
            return True

    def events(self):
        with self.connection() as conn:
            return [parse_json(row[0]) for row in conn.execute("SELECT payload FROM inbox ORDER BY rowid")]


def deliver(engine, inbox, limit=100, *, _after_store=None):
    stored = replayed = 0
    events = engine.pending(limit)
    for event in events:
        if inbox.store(event):
            stored += 1
        else:
            replayed += 1
        if _after_store:
            _after_store(event)  # Test-only crash boundary; not an HTTP feature.
        engine.acknowledge(event["event_id"])
    return {"attempted": len(events), "new_inbox_events": stored, "duplicate_deliveries": replayed}
