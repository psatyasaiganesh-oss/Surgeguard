from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any

from .errors import ValidationError


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
MAX_KEY_BYTES = 512
MAX_VALUE_BYTES = 1_048_576
MAX_BATCH_MUTATIONS = 100


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
    if len(rendered.encode()) > 4_194_304:
        raise ValidationError("canonical JSON exceeds 4 MiB")
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


def require_closed_object(
    value: Any, name: str, required: set[str], optional: set[str] | None = None
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    optional = optional or set()
    missing = required - set(value)
    unknown = set(value) - required - optional
    if missing:
        raise ValidationError(f"{name} is missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ValidationError(f"{name} has unknown fields: {', '.join(sorted(unknown))}")
    return value


def require_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(
            f"{name} must be 1-128 characters using letters, numbers, '.', '_', ':', or '-'"
        )
    return value


def require_int(
    value: Any, name: str, *, minimum: int | None = None, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{name} must be at most {maximum}")
    return value


def require_key(value: Any, name: str = "key") -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValidationError(f"{name} must be a non-empty string without NUL bytes")
    if len(value.encode()) > MAX_KEY_BYTES:
        raise ValidationError(f"{name} exceeds {MAX_KEY_BYTES} UTF-8 bytes")
    return value


def require_value(value: Any, name: str = "value") -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{name} must be a string")
    if len(value.encode()) > MAX_VALUE_BYTES:
        raise ValidationError(f"{name} exceeds {MAX_VALUE_BYTES} UTF-8 bytes")
    return value


def require_hex_digest(value: Any, name: str = "digest") -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True)
class VersionedRecord:
    key: str
    sequence: int
    value: str | None
    tombstone: bool

    @classmethod
    def from_value(cls, value: Any) -> "VersionedRecord":
        body = require_closed_object(
            value, "record", {"key", "sequence", "value", "tombstone"}
        )
        key = require_key(body["key"])
        sequence = require_int(body["sequence"], "sequence", minimum=1)
        tombstone = body["tombstone"]
        if not isinstance(tombstone, bool):
            raise ValidationError("tombstone must be boolean")
        if tombstone:
            if body["value"] is not None:
                raise ValidationError("a tombstone value must be null")
            normalized_value = None
        else:
            normalized_value = require_value(body["value"])
        return cls(key, sequence, normalized_value, tombstone)

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "sequence": self.sequence,
            "value": self.value,
            "tombstone": self.tombstone,
        }


def normalize_mutations(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValidationError("mutations must be a non-empty array")
    if len(value) > MAX_BATCH_MUTATIONS:
        raise ValidationError(f"mutations exceeds {MAX_BATCH_MUTATIONS} entries")
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        body = require_closed_object(raw, f"mutation[{index}]", {"operation", "key"}, {"value"})
        operation = body["operation"]
        if operation not in {"PUT", "DELETE"}:
            raise ValidationError("operation must be PUT or DELETE")
        key = require_key(body["key"], f"mutation[{index}].key")
        if operation == "PUT":
            if "value" not in body:
                raise ValidationError("PUT requires value")
            normalized.append(
                {"operation": operation, "key": key, "value": require_value(body["value"])}
            )
        else:
            if "value" in body:
                raise ValidationError("DELETE does not accept value")
            normalized.append({"operation": operation, "key": key})
    return normalized


def require_finite_number(value: Any, name: str, *, minimum: float = 0) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or float(value) < minimum
    ):
        raise ValidationError(f"{name} must be a finite number >= {minimum}")
    return float(value)
