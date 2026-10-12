"""Strict event parsing for the project's deliberately small event contract."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass


EVENT_TYPES = {
    "payment_intent.created": "created",
    "payment_intent.succeeded": "succeeded",
    "payment_intent.payment_failed": "failed",
}
ID = re.compile(r"^(?:evt|pi)_[A-Za-z0-9_-]{4,80}$")
CURRENCY = re.compile(r"^[a-z]{3}$")
MAX_PAYLOAD_BYTES = 64 * 1024


class SchemaError(ValueError):
    pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SchemaError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    payment_id: str
    resource_version: int
    status: str
    amount: int
    currency: str
    created: int

    @property
    def effect_key(self) -> str:
        # The version is synthetic in this project. In a Stripe integration,
        # fetch current object state instead of assuming webhook order.
        return f"{self.event_type}:{self.payment_id}:v{self.resource_version}"


def parse_event(raw: bytes) -> Event:
    if not isinstance(raw, bytes):
        raise TypeError("raw payload must be bytes")
    if not raw or len(raw) > MAX_PAYLOAD_BYTES:
        raise SchemaError(f"payload must contain 1 to {MAX_PAYLOAD_BYTES} bytes")
    try:
        data = json.loads(raw, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError("payload must be valid UTF-8 JSON") from exc
    if not isinstance(data, dict) or set(data) != {"id", "type", "created", "data"}:
        raise SchemaError("event must contain exactly id, type, created, and data")
    if not isinstance(data["id"], str) or not ID.fullmatch(data["id"]) or not data["id"].startswith("evt_"):
        raise SchemaError("invalid event id")
    event_type = data["type"]
    if event_type not in EVENT_TYPES:
        raise SchemaError("unsupported event type")
    if type(data["created"]) is not int or data["created"] < 0:
        raise SchemaError("created must be a nonnegative integer")
    envelope = data["data"]
    if not isinstance(envelope, dict) or set(envelope) != {"object"}:
        raise SchemaError("data must contain exactly object")
    obj = envelope["object"]
    expected = {"id", "version", "status", "amount", "currency"}
    if not isinstance(obj, dict) or set(obj) != expected:
        raise SchemaError("payment object has an invalid shape")
    if not isinstance(obj["id"], str) or not ID.fullmatch(obj["id"]) or not obj["id"].startswith("pi_"):
        raise SchemaError("invalid payment id")
    if type(obj["version"]) is not int or not 1 <= obj["version"] <= 1_000_000:
        raise SchemaError("version must be an integer in [1, 1000000]")
    expected_status = EVENT_TYPES[event_type]
    if obj["status"] != expected_status:
        raise SchemaError("event type and object status disagree")
    if type(obj["amount"]) is not int or not 0 <= obj["amount"] <= 999_999_999:
        raise SchemaError("amount must be an integer in [0, 999999999]")
    if not isinstance(obj["currency"], str) or not CURRENCY.fullmatch(obj["currency"]):
        raise SchemaError("currency must be a lowercase three-letter code")
    return Event(
        event_id=data["id"],
        event_type=event_type,
        payment_id=obj["id"],
        resource_version=obj["version"],
        status=obj["status"],
        amount=obj["amount"],
        currency=obj["currency"],
        created=data["created"],
    )
