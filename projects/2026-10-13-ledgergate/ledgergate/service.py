"""Secure intake boundary separated from asynchronous business processing."""

from __future__ import annotations

from dataclasses import dataclass

from .schema import SchemaError, parse_event
from .security import SignatureError, verify_signature
from .store import SQLiteStore


@dataclass(frozen=True)
class Receipt:
    http_status: int
    result: str
    event_id: str | None = None

    def to_dict(self) -> dict:
        return {"status": self.result, "event_id": self.event_id}


class LedgerGate:
    def __init__(self, store: SQLiteStore, secrets: list[bytes] | tuple[bytes, ...], *, tolerance_seconds: int = 300):
        self.store = store
        self.secrets = tuple(secrets)
        self.tolerance_seconds = tolerance_seconds
        # Fail at startup instead of accepting a broken security configuration.
        if not self.secrets:
            raise ValueError("at least one signing secret is required")
        for secret in self.secrets:
            if not isinstance(secret, bytes) or len(secret) < 16:
                raise ValueError("signing secrets must contain at least 16 bytes")
        if type(tolerance_seconds) is not int or tolerance_seconds <= 0:
            raise ValueError("tolerance_seconds must be a positive integer")

    def accept(self, raw_payload: bytes, signature_header: str, *, now: int | None = None) -> Receipt:
        try:
            verify_signature(
                raw_payload,
                signature_header,
                self.secrets,
                now=now,
                tolerance_seconds=self.tolerance_seconds,
            )
        except SignatureError:
            return Receipt(400, "invalid_signature")
        try:
            event = parse_event(raw_payload)
        except SchemaError:
            return Receipt(400, "invalid_payload")
        result = self.store.ingest(event, raw_payload)
        status = 409 if result == "event_conflict" else (202 if result == "accepted" else 200)
        return Receipt(status, result, event.event_id)

    def drain(self, *, now: float | None = None) -> dict:
        processed = self.store.process_all(now=now)
        dispatched = self.store.dispatch_all(now=now, lease_seconds=0)
        return {"events_processed": processed, "outbox_dispatched": dispatched, "snapshot": self.store.snapshot()}
