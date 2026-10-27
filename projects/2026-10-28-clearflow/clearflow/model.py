from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import ValidationError


IDENTIFIER = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$")
CURRENCY = re.compile(r"^[A-Z]{3}$")
MAX_POSTINGS = 64
MAX_EXPECTED_REFERENCES = 10_000


def require_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(
            f"{name} must be 1-128 characters using letters, numbers, '.', '_', ':', or '-'"
        )
    return value


def require_currency(value: Any) -> str:
    if not isinstance(value, str) or not CURRENCY.fullmatch(value):
        raise ValidationError("currency must be a three-letter uppercase code")
    return value


def require_int(value: Any, name: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{name} must be at least {minimum}")
    return value


def require_timestamp(value: Any, name: str) -> int:
    return require_int(value, name, minimum=0)


def require_closed_object(
    value: Any, name: str, required: set[str], optional: set[str] | None = None
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    optional = optional or set()
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        raise ValidationError(f"{name} is missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ValidationError(f"{name} has unknown fields: {', '.join(sorted(unknown))}")
    return value


def canonical_json(value: Any) -> str:
    try:
        rendered = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError("value is not canonical JSON") from exc
    if len(rendered.encode("utf-8")) > 1_048_576:
        raise ValidationError("canonical JSON exceeds 1 MiB")
    return rendered


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def parse_json(raw: str, name: str = "JSON") -> Any:
    def reject_constant(value: str) -> None:
        raise ValidationError(f"{name} contains non-finite number {value}")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"{name} contains duplicate field {key}")
            result[key] = value
        return result

    try:
        return json.loads(
            raw,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicates,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValidationError(f"invalid {name}") from exc


@dataclass(frozen=True)
class Posting:
    account_id: str
    amount_minor: int

    @classmethod
    def from_value(cls, value: Any) -> "Posting":
        body = require_closed_object(value, "posting", {"account_id", "amount_minor"})
        amount = require_int(body["amount_minor"], "amount_minor")
        if amount == 0:
            raise ValidationError("posting amount_minor cannot be zero")
        if abs(amount) > 9_000_000_000_000_000:
            raise ValidationError("posting amount_minor is out of range")
        return cls(require_identifier(body["account_id"], "account_id"), amount)

    def as_dict(self) -> dict[str, Any]:
        return {"account_id": self.account_id, "amount_minor": self.amount_minor}


@dataclass(frozen=True)
class MoneyEvent:
    event_id: str
    producer: str
    reference_id: str
    currency: str
    occurred_ns: int
    postings: tuple[Posting, ...]
    metadata: dict[str, str]

    @classmethod
    def from_value(cls, value: Any) -> "MoneyEvent":
        body = require_closed_object(
            value,
            "event",
            {"event_id", "producer", "reference_id", "currency", "occurred_ns", "postings"},
            {"metadata"},
        )
        raw_postings = body["postings"]
        if not isinstance(raw_postings, list) or not 2 <= len(raw_postings) <= MAX_POSTINGS:
            raise ValidationError(f"postings must contain 2-{MAX_POSTINGS} entries")
        postings = tuple(Posting.from_value(item) for item in raw_postings)
        if sum(item.amount_minor for item in postings) != 0:
            raise ValidationError("double-entry postings must sum to zero")
        if len({item.account_id for item in postings}) != len(postings):
            raise ValidationError("an event may post to an account only once")
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict) or len(metadata) > 32:
            raise ValidationError("metadata must be an object with at most 32 entries")
        normalized_metadata: dict[str, str] = {}
        for key, item in metadata.items():
            require_identifier(key, "metadata key")
            if not isinstance(item, str) or len(item.encode()) > 512:
                raise ValidationError("metadata values must be strings of at most 512 bytes")
            normalized_metadata[key] = item
        canonical_json(normalized_metadata)
        return cls(
            require_identifier(body["event_id"], "event_id"),
            require_identifier(body["producer"], "producer"),
            require_identifier(body["reference_id"], "reference_id"),
            require_currency(body["currency"]),
            require_timestamp(body["occurred_ns"], "occurred_ns"),
            postings,
            normalized_metadata,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "producer": self.producer,
            "reference_id": self.reference_id,
            "currency": self.currency,
            "occurred_ns": self.occurred_ns,
            "postings": [item.as_dict() for item in self.postings],
            "metadata": dict(sorted(self.metadata.items())),
        }


@dataclass(frozen=True)
class QualityPolicy:
    max_lag_ns: int = 300_000_000_000
    clearing_grace_ns: int = 300_000_000_000
    max_missing_references: int = 0
    minimum_score: float = 100.0

    @classmethod
    def from_value(cls, value: Any) -> "QualityPolicy":
        body = require_closed_object(
            value,
            "policy",
            {"max_lag_ns", "clearing_grace_ns", "max_missing_references", "minimum_score"},
        )
        score = body["minimum_score"]
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
            raise ValidationError("minimum_score must be finite")
        if not 0 <= float(score) <= 100:
            raise ValidationError("minimum_score must be between 0 and 100")
        return cls(
            require_int(body["max_lag_ns"], "max_lag_ns", minimum=0),
            require_int(body["clearing_grace_ns"], "clearing_grace_ns", minimum=0),
            require_int(body["max_missing_references"], "max_missing_references", minimum=0),
            float(score),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_lag_ns": self.max_lag_ns,
            "clearing_grace_ns": self.clearing_grace_ns,
            "max_missing_references": self.max_missing_references,
            "minimum_score": self.minimum_score,
        }


def normalize_references(values: Any) -> tuple[str, ...]:
    if not isinstance(values, list) or not values:
        raise ValidationError("expected_references must be a non-empty array")
    if len(values) > MAX_EXPECTED_REFERENCES:
        raise ValidationError(f"expected_references exceeds {MAX_EXPECTED_REFERENCES}")
    result = tuple(require_identifier(value, "reference_id") for value in values)
    if len(set(result)) != len(result):
        raise ValidationError("expected_references must be unique")
    return tuple(sorted(result))


def validate_account_kind(value: Any) -> str:
    if value not in {"clearing", "terminal"}:
        raise ValidationError("account kind must be clearing or terminal")
    return value


def validate_actor(value: Any, name: str = "actor") -> str:
    return require_identifier(value, name)


def ensure_distinct(values: Iterable[str], name: str) -> tuple[str, ...]:
    result = tuple(values)
    if len(set(result)) != len(result):
        raise ValidationError(f"{name} must be distinct")
    return result
