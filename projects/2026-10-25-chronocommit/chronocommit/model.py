"""Validated immutable models and deterministic serialization."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import ValidationError


ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
MAX_MUTATIONS = 64
MAX_JSON_BYTES = 16_384
MAX_TIMESTAMP_NS = 9_223_372_036_854_775_807


def canonical_json(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError("value must be finite JSON") from exc
    if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
        raise ValidationError(f"JSON value exceeds {MAX_JSON_BYTES} bytes")
    return encoded


def validate_id(value: str, field: str) -> str:
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValidationError(
            f"{field} must match {ID_RE.pattern} and be at most 128 characters"
        )
    return value


def validate_timestamp(value: int, field: str = "timestamp") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} must be an integer")
    if value < 0 or value > MAX_TIMESTAMP_NS:
        raise ValidationError(f"{field} is outside the supported range")
    return value


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Mutation:
    key: str
    value: Any
    expected_ts: int
    delete: bool = False

    def validated(self) -> "Mutation":
        validate_id(self.key, "key")
        validate_timestamp(self.expected_ts, "expected_ts")
        if not isinstance(self.delete, bool):
            raise ValidationError("delete must be boolean")
        if self.delete and self.value is not None:
            raise ValidationError("a delete mutation cannot include a value")
        canonical_json(None if self.delete else self.value)
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "delete": self.delete,
            "expected_ts": self.expected_ts,
            "key": self.key,
            "value": None if self.delete else self.value,
        }


def validate_mutations(mutations: Iterable[Mutation]) -> tuple[Mutation, ...]:
    result = tuple(m.validated() for m in mutations)
    if not result:
        raise ValidationError("a transaction requires at least one mutation")
    if len(result) > MAX_MUTATIONS:
        raise ValidationError(f"a transaction supports at most {MAX_MUTATIONS} mutations")
    keys = [m.key for m in result]
    if len(keys) != len(set(keys)):
        raise ValidationError("a transaction cannot mutate the same key twice")
    return tuple(sorted(result, key=lambda mutation: mutation.key))


@dataclass(frozen=True)
class VersionedValue:
    key: str
    value: Any
    commit_ts: int
    deleted: bool = False
    txn_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "commit_ts": self.commit_ts,
            "deleted": self.deleted,
            "key": self.key,
            "txn_id": self.txn_id,
            "value": None if self.deleted else self.value,
        }


@dataclass(frozen=True)
class CommitResult:
    txn_id: str
    tenant: str
    idempotency_key: str
    commit_ts: int
    participant_count: int
    replayed: bool
    acknowledged_ns: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "acknowledged_ns": self.acknowledged_ns,
            "commit_ts": self.commit_ts,
            "idempotency_key": self.idempotency_key,
            "participant_count": self.participant_count,
            "replayed": self.replayed,
            "tenant": self.tenant,
            "txn_id": self.txn_id,
        }

