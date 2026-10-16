"""Strict input validation and stable serialization; amounts use integer cents."""

from dataclasses import dataclass
import hashlib
import json
import re

from .errors import ValidationError

IDENTIFIER = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
SUB_ID = re.compile(r"sub_[A-Za-z0-9_-]{1,64}\Z")
PLAN = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
FIELDS = {"id", "plan", "amount_cents", "currency", "status"}


def integer(value, name, minimum=0, maximum=2**63 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def identifier(value, name="operation_id"):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(f"{name} must be 1..128 ASCII identifier characters")
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def subscriptions(value):
    if not isinstance(value, list) or len(value) > 100:
        raise ValidationError("subscriptions must be a list of at most 100 entries")
    result, seen = [], set()
    for item in value:
        if not isinstance(item, dict) or set(item) != FIELDS:
            raise ValidationError(f"subscription requires exactly {sorted(FIELDS)}")
        sid = item["id"]
        if not isinstance(sid, str) or not SUB_ID.fullmatch(sid):
            raise ValidationError("subscription id must start with sub_ and use ASCII identifiers")
        if sid in seen:
            raise ValidationError("duplicate subscription id in customer")
        seen.add(sid)
        if not isinstance(item["plan"], str) or not PLAN.fullmatch(item["plan"]):
            raise ValidationError("invalid plan identifier")
        integer(item["amount_cents"], "amount_cents", 0, 10**12)
        if not isinstance(item["currency"], str) or not re.fullmatch(r"[A-Z]{3}", item["currency"]):
            raise ValidationError("currency must contain three uppercase ASCII letters")
        if item["status"] not in ("active", "paused", "canceled"):
            raise ValidationError("invalid subscription status")
        result.append(dict(item))
    return sorted(result, key=lambda s: s["id"])


@dataclass(frozen=True)
class Snapshot:
    customer_id: int
    revision: int
    deleted: bool
    payload: str

    def record(self):
        return {"customer_id": self.customer_id, "revision": self.revision,
                "deleted": self.deleted, "subscriptions": json.loads(self.payload)}


@dataclass(frozen=True)
class Batch:
    """Trusted internal snapshot, not an interchange format for untrusted callers."""

    owner: str
    fence: int
    cursor_before: int
    cursor_after: int
    watermark: int
    rows: tuple[Snapshot, ...]
