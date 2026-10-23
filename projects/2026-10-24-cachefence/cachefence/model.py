"""Strict data models shared by the origin, invalidation pipeline, and cache."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any, Mapping

MAX_BODY_BYTES = 1_048_576
MAX_VALUE_BYTES = 65_536
MAX_VALUE_DEPTH = 8
MAX_COLLECTION_ITEMS = 256
MAX_STRING_LENGTH = 32_768
MAX_VERSION = 2**63 - 1

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class ValidationError(ValueError):
    """External input violates a closed schema or resource bound."""


class ConflictError(RuntimeError):
    """An optimistic concurrency precondition is stale."""


class IntegrityError(RuntimeError):
    """Content, identity, signature, or persisted state is inconsistent."""


class AuthorizationError(RuntimeError):
    """A capability token is absent or invalid."""


class LeaseExpiredError(RuntimeError):
    """A worker tried to acknowledge work after its lease expired."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValidationError(f"non-finite number is not valid JSON: {value}")


def strict_loads(data: str | bytes, *, max_bytes: int = MAX_BODY_BYTES) -> Any:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    if len(raw) > max_bytes:
        raise ValidationError(f"JSON body exceeds {max_bytes} bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError("JSON body must be UTF-8") from exc
    try:
        return json.loads(
            text,
            object_pairs_hook=_no_duplicates,
            parse_constant=_reject_constant,
        )
    except ValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise ValidationError(f"invalid JSON: {exc}") from exc


def require_keys(
    value: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str] | None = None,
    label: str,
) -> None:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be an object")
    optional = optional or set()
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        raise ValidationError(f"{label} missing fields: {sorted(missing)}")
    if unknown:
        raise ValidationError(f"{label} has unknown fields: {sorted(unknown)}")


def validate_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise ValidationError(
            f"{label} must start with an alphanumeric and contain only letters, "
            "digits, dot, underscore, or hyphen (maximum 96 characters)"
        )
    return value


def validate_digest(value: Any, label: str = "digest") -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValidationError(f"{label} must be a lowercase SHA-256 digest")
    return value


def validate_version(value: Any, label: str = "version") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{label} must be an integer")
    if value < 1 or value > MAX_VERSION:
        raise ValidationError(f"{label} is outside the supported range")
    return value


def validate_ns(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{label} must be an integer")
    if value < 0 or value > MAX_VERSION:
        raise ValidationError(f"{label} is outside the supported range")
    return value


def validate_json_value(value: Any, *, depth: int = 0) -> None:
    if depth > MAX_VALUE_DEPTH:
        raise ValidationError(f"JSON value nesting exceeds {MAX_VALUE_DEPTH}")
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > MAX_VERSION:
            raise ValidationError("integer is outside the signed 64-bit range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("floating-point values must be finite")
        return
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            raise ValidationError(f"string exceeds {MAX_STRING_LENGTH} characters")
        return
    if isinstance(value, list):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ValidationError(f"list exceeds {MAX_COLLECTION_ITEMS} items")
        for item in value:
            validate_json_value(item, depth=depth + 1)
        return
    if isinstance(value, dict):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ValidationError(f"object exceeds {MAX_COLLECTION_ITEMS} keys")
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 128:
                raise ValidationError("object keys must be non-empty strings <= 128 chars")
            validate_json_value(item, depth=depth + 1)
        return
    raise ValidationError(f"unsupported JSON value type: {type(value).__name__}")


def content_digest(*, deleted: bool, value: Any = None) -> str:
    if not isinstance(deleted, bool):
        raise ValidationError("deleted must be boolean")
    if deleted:
        if value is not None:
            raise ValidationError("deleted content cannot include a value")
        material = {"deleted": True}
    else:
        validate_json_value(value)
        material = {"deleted": False, "value": value}
    encoded = canonical_json(material).encode("utf-8")
    if len(encoded) > MAX_VALUE_BYTES:
        raise ValidationError(f"value exceeds {MAX_VALUE_BYTES} encoded bytes")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class Snapshot:
    tenant: str
    key: str
    version: int
    digest: str
    deleted: bool
    value: Any
    updated_ns: int

    def validate(self) -> None:
        validate_id(self.tenant, "tenant")
        validate_id(self.key, "key")
        validate_version(self.version)
        validate_digest(self.digest)
        validate_ns(self.updated_ns, "updated_ns")
        if not isinstance(self.deleted, bool):
            raise ValidationError("deleted must be boolean")
        if self.deleted and self.value is not None:
            raise ValidationError("deleted snapshot cannot contain value")
        expected = content_digest(deleted=self.deleted, value=self.value)
        if not self.deleted:
            validate_json_value(self.value)
        if expected != self.digest:
            raise IntegrityError("snapshot digest does not match its content")

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "tenant": self.tenant,
            "key": self.key,
            "version": self.version,
            "digest": self.digest,
            "deleted": self.deleted,
            "updated_ns": self.updated_ns,
        }
        if not self.deleted:
            result["value"] = self.value
        return result

    @classmethod
    def from_dict(cls, value: Any) -> "Snapshot":
        if not isinstance(value, dict):
            raise ValidationError("snapshot must be an object")
        require_keys(
            value,
            required={"tenant", "key", "version", "digest", "deleted", "updated_ns"},
            optional={"value"},
            label="snapshot",
        )
        if not isinstance(value["deleted"], bool):
            raise ValidationError("deleted must be boolean")
        if value["deleted"] and "value" in value:
            raise ValidationError("deleted snapshot cannot contain value")
        if not value["deleted"] and "value" not in value:
            raise ValidationError("live snapshot must contain value")
        snapshot = cls(
            tenant=validate_id(value["tenant"], "tenant"),
            key=validate_id(value["key"], "key"),
            version=validate_version(value["version"]),
            digest=validate_digest(value["digest"]),
            deleted=value["deleted"],
            value=value.get("value"),
            updated_ns=validate_ns(value["updated_ns"], "updated_ns"),
        )
        snapshot.validate()
        return snapshot


@dataclass(frozen=True)
class InvalidationEvent:
    origin: str
    tenant: str
    key: str
    version: int
    digest: str
    deleted: bool
    created_ns: int

    @property
    def event_id(self) -> str:
        identity = canonical_json(
            {
                "origin": self.origin,
                "tenant": self.tenant,
                "key": self.key,
                "version": self.version,
            }
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def validate(self) -> None:
        validate_id(self.origin, "origin")
        validate_id(self.tenant, "tenant")
        validate_id(self.key, "key")
        validate_version(self.version)
        validate_digest(self.digest)
        validate_ns(self.created_ns, "created_ns")
        if not isinstance(self.deleted, bool):
            raise ValidationError("deleted must be boolean")

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": 1,
            "event_id": self.event_id,
            "origin": self.origin,
            "tenant": self.tenant,
            "key": self.key,
            "version": self.version,
            "digest": self.digest,
            "deleted": self.deleted,
            "created_ns": self.created_ns,
        }

    def to_json(self) -> str:
        self.validate()
        return canonical_json(self.as_dict())

    @classmethod
    def from_dict(cls, value: Any) -> "InvalidationEvent":
        if not isinstance(value, dict):
            raise ValidationError("invalidation event must be an object")
        require_keys(
            value,
            required={
                "schema",
                "event_id",
                "origin",
                "tenant",
                "key",
                "version",
                "digest",
                "deleted",
                "created_ns",
            },
            label="invalidation event",
        )
        if value["schema"] != 1 or isinstance(value["schema"], bool):
            raise ValidationError("unsupported invalidation schema")
        event = cls(
            origin=validate_id(value["origin"], "origin"),
            tenant=validate_id(value["tenant"], "tenant"),
            key=validate_id(value["key"], "key"),
            version=validate_version(value["version"]),
            digest=validate_digest(value["digest"]),
            deleted=value["deleted"],
            created_ns=validate_ns(value["created_ns"], "created_ns"),
        )
        event.validate()
        if value["event_id"] != event.event_id:
            raise ValidationError("event_id does not match event identity")
        return event

    @classmethod
    def from_json(cls, data: str | bytes) -> "InvalidationEvent":
        return cls.from_dict(strict_loads(data))


def snapshot_for(
    *, tenant: str, key: str, version: int, deleted: bool, value: Any, updated_ns: int | None = None
) -> Snapshot:
    snapshot = Snapshot(
        tenant=validate_id(tenant, "tenant"),
        key=validate_id(key, "key"),
        version=validate_version(version),
        digest=content_digest(deleted=deleted, value=value),
        deleted=deleted,
        value=None if deleted else value,
        updated_ns=time.time_ns() if updated_ns is None else validate_ns(updated_ns, "updated_ns"),
    )
    snapshot.validate()
    return snapshot
