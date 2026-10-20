"""A separate idempotent actuator store that models network-device state."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .model import ValidationError, canonical, parse_topology


class ActuatorConflict(RuntimeError):
    """The physical state changed outside the approved execution."""


class Actuator:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def _connect(self) -> sqlite3.Connection:
        existed = os.path.exists(self.path)
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        if not existed and self.path != ":memory:":
            os.chmod(self.path, 0o600)
        return connection

    def initialize(self, topology_value: Any) -> None:
        topology = parse_topology(topology_value)
        static = {
            "nodes": topology["nodes"],
            "links": [{key: value for key, value in link.items() if key != "up"} for link in topology["links"]],
            "requirements": topology["requirements"],
        }
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta(
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    version INTEGER NOT NULL,
                    document TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS link_state(
                    link_id TEXT PRIMARY KEY,
                    up INTEGER NOT NULL CHECK(up IN (0, 1))
                );
                CREATE TABLE IF NOT EXISTS receipts(
                    operation_id TEXT PRIMARY KEY,
                    link_id TEXT NOT NULL,
                    requested_up INTEGER NOT NULL,
                    expected_version INTEGER NOT NULL,
                    previous_up INTEGER NOT NULL,
                    new_version INTEGER NOT NULL
                );
                """
            )
            row = connection.execute("SELECT version, document FROM meta WHERE singleton = 1").fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO meta(singleton, version, document) VALUES(1, ?, ?)",
                    (topology["version"], canonical(static)),
                )
                connection.executemany(
                    "INSERT INTO link_state(link_id, up) VALUES(?, ?)",
                    [(link["id"], int(link["up"])) for link in topology["links"]],
                )
            elif row["document"] != canonical(static):
                raise ValidationError("actuator already contains a different topology")

    def snapshot(self) -> dict[str, Any]:
        with self._connect() as connection:
            meta = connection.execute("SELECT version, document FROM meta WHERE singleton = 1").fetchone()
            if meta is None:
                raise RuntimeError("actuator is not initialized")
            static = json.loads(meta["document"])
            states = {
                row["link_id"]: bool(row["up"])
                for row in connection.execute("SELECT link_id, up FROM link_state")
            }
        links = []
        for link in static["links"]:
            item = dict(link)
            item["up"] = states[item["id"]]
            links.append(item)
        return parse_topology({
            "version": int(meta["version"]),
            "nodes": static["nodes"],
            "links": links,
            "requirements": static["requirements"],
        })

    def receipt(self, operation_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM receipts WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            return dict(row) if row else None

    def apply(
        self,
        operation_id: str,
        link_id: str,
        requested_up: bool,
        expected_version: int,
    ) -> dict[str, Any]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                "SELECT * FROM receipts WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            if receipt is not None:
                if (
                    receipt["link_id"] != link_id
                    or bool(receipt["requested_up"]) != requested_up
                    or receipt["expected_version"] != expected_version
                ):
                    raise ActuatorConflict("operation id was reused with different parameters")
                result = dict(receipt)
                result["replayed"] = True
                return result

            meta = connection.execute("SELECT version FROM meta WHERE singleton = 1").fetchone()
            if meta is None:
                raise RuntimeError("actuator is not initialized")
            actual_version = int(meta["version"])
            if actual_version != expected_version:
                raise ActuatorConflict(
                    f"topology drift: expected version {expected_version}, found {actual_version}"
                )
            state = connection.execute(
                "SELECT up FROM link_state WHERE link_id = ?", (link_id,)
            ).fetchone()
            if state is None:
                raise ActuatorConflict(f"unknown link: {link_id}")
            previous_up = bool(state["up"])
            if previous_up == requested_up:
                raise ActuatorConflict(f"operation would not change link {link_id}")
            new_version = actual_version + 1
            connection.execute(
                "UPDATE link_state SET up = ? WHERE link_id = ?",
                (int(requested_up), link_id),
            )
            connection.execute(
                "UPDATE meta SET version = ? WHERE singleton = 1", (new_version,)
            )
            connection.execute(
                """
                INSERT INTO receipts(
                    operation_id, link_id, requested_up, expected_version, previous_up, new_version
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    link_id,
                    int(requested_up),
                    expected_version,
                    int(previous_up),
                    new_version,
                ),
            )
            return {
                "operation_id": operation_id,
                "link_id": link_id,
                "requested_up": int(requested_up),
                "expected_version": expected_version,
                "previous_up": int(previous_up),
                "new_version": new_version,
                "replayed": False,
            }

    def external_change(self, link_id: str, up: bool) -> int:
        """Test/demo hook that represents an out-of-band device change."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT up FROM link_state WHERE link_id = ?", (link_id,)
            ).fetchone()
            if row is None:
                raise ActuatorConflict(f"unknown link: {link_id}")
            if bool(row["up"]) == up:
                raise ActuatorConflict("external change must alter state")
            version = int(
                connection.execute("SELECT version FROM meta WHERE singleton = 1").fetchone()["version"]
            ) + 1
            connection.execute("UPDATE link_state SET up = ? WHERE link_id = ?", (int(up), link_id))
            connection.execute("UPDATE meta SET version = ? WHERE singleton = 1", (version,))
            return version
