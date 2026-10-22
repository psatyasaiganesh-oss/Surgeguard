"""Strict wire models and vector-clock helpers for MergeMesh."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any, Mapping

MAX_BODY_BYTES = 1_048_576
MAX_OPERATION_BYTES = 65_536
MAX_FIELDS_PER_OPERATION = 64
MAX_CLOCK_ENTRIES = 64
MAX_VALUE_DEPTH = 8
MAX_STRING_LENGTH = 16_384
MAX_COLLECTION_ITEMS = 256
MAX_SEQUENCE = 2**63 - 1

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_FIELD_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")


class ValidationError(ValueError):
    """Input does not satisfy the closed MergeMesh schema."""


class ConflictError(RuntimeError):
    """A precondition failed because the document changed."""


class IntegrityError(RuntimeError):
    """Authenticated or persisted data failed an integrity check."""


def canonical_json(value: Any) -> str:
    """Return a stable JSON representation suitable for hashing and signing."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValidationError(f"non-finite number is not valid JSON: {value}")


def strict_loads(data: str | bytes, *, max_bytes: int = MAX_BODY_BYTES) -> Any:
    """Parse JSON while rejecting oversize bodies, duplicates, and NaN/Infinity."""

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
            object_pairs_hook=_pairs_no_duplicates,
            parse_constant=_reject_constant,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValidationError(f"invalid JSON: {exc}") from exc


def require_exact_keys(
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


def validate_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise ValidationError(
            f"{label} must match {_ID_RE.pattern} and be at most 64 characters"
        )
    return value


def validate_field_name(value: Any) -> str:
    if not isinstance(value, str) or not _FIELD_RE.fullmatch(value):
        raise ValidationError(
            f"field name must match {_FIELD_RE.pattern} and be at most 64 characters"
        )
    return value


def validate_json_value(value: Any, *, depth: int = 0) -> None:
    """Validate a bounded JSON value without changing its semantics."""

    if depth > MAX_VALUE_DEPTH:
        raise ValidationError(f"JSON value nesting exceeds {MAX_VALUE_DEPTH}")
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > MAX_SEQUENCE:
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
                raise ValidationError("nested object keys must be non-empty strings <= 128 chars")
            validate_json_value(item, depth=depth + 1)
        return
    raise ValidationError(f"unsupported JSON value type: {type(value).__name__}")


def normalize_clock(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        raise ValidationError("context must be an object")
    if len(value) > MAX_CLOCK_ENTRIES:
        raise ValidationError(f"context exceeds {MAX_CLOCK_ENTRIES} regions")
    clock: dict[str, int] = {}
    for region, sequence in value.items():
        region = validate_identifier(region, "context region")
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise ValidationError("context sequences must be integers")
        if sequence < 0 or sequence > MAX_SEQUENCE:
            raise ValidationError("context sequence is outside the allowed range")
        if sequence:
            clock[region] = sequence
    return dict(sorted(clock.items()))


def join_clocks(*clocks: Mapping[str, int]) -> dict[str, int]:
    joined: dict[str, int] = {}
    for clock in clocks:
        for region, sequence in clock.items():
            joined[region] = max(joined.get(region, 0), sequence)
    return dict(sorted((region, seq) for region, seq in joined.items() if seq))


def compare_clocks(left: Mapping[str, int], right: Mapping[str, int]) -> str:
    """Compare clocks, returning equal, before, after, or concurrent."""

    regions = set(left) | set(right)
    left_le = all(left.get(region, 0) <= right.get(region, 0) for region in regions)
    right_le = all(right.get(region, 0) <= left.get(region, 0) for region in regions)
    if left_le and right_le:
        return "equal"
    if left_le:
        return "before"
    if right_le:
        return "after"
    return "concurrent"


@dataclass(frozen=True)
class FieldMutation:
    deleted: bool
    value: Any = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"deleted": self.deleted}
        if not self.deleted:
            result["value"] = self.value
        return result

    @classmethod
    def from_dict(cls, value: Any) -> "FieldMutation":
        if not isinstance(value, dict):
            raise ValidationError("field mutation must be an object")
        require_exact_keys(
            value,
            required={"deleted"},
            optional={"value"},
            label="field mutation",
        )
        if not isinstance(value["deleted"], bool):
            raise ValidationError("field mutation deleted must be boolean")
        deleted = value["deleted"]
        if deleted and "value" in value:
            raise ValidationError("deleted field mutation cannot include value")
        if not deleted and "value" not in value:
            raise ValidationError("non-deleted field mutation must include value")
        field_value = value.get("value")
        if not deleted:
            validate_json_value(field_value)
        return cls(deleted=deleted, value=field_value)


@dataclass(frozen=True)
class Operation:
    tenant: str
    document: str
    origin: str
    sequence: int
    context: dict[str, int]
    mutations: dict[str, FieldMutation]
    created_ns: int

    @property
    def op_id(self) -> str:
        return f"{self.origin}:{self.sequence}"

    @property
    def clock(self) -> dict[str, int]:
        return join_clocks(self.context, {self.origin: self.sequence})

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": 1,
            "op_id": self.op_id,
            "tenant": self.tenant,
            "document": self.document,
            "origin": self.origin,
            "sequence": self.sequence,
            "context": self.context,
            "mutations": {
                field: mutation.as_dict()
                for field, mutation in sorted(self.mutations.items())
            },
            "created_ns": self.created_ns,
        }

    def to_json(self) -> str:
        encoded = canonical_json(self.as_dict())
        if len(encoded.encode("utf-8")) > MAX_OPERATION_BYTES:
            raise ValidationError(f"operation exceeds {MAX_OPERATION_BYTES} bytes")
        return encoded

    @classmethod
    def create(
        cls,
        *,
        tenant: str,
        document: str,
        origin: str,
        sequence: int,
        context: Mapping[str, int],
        changes: Mapping[str, Any] | None = None,
        delete_fields: list[str] | tuple[str, ...] | None = None,
        created_ns: int | None = None,
    ) -> "Operation":
        changes = changes or {}
        delete_fields = delete_fields or []
        if not isinstance(changes, Mapping):
            raise ValidationError("changes must be an object")
        if not isinstance(delete_fields, (list, tuple)):
            raise ValidationError("delete_fields must be a list")
        mutations: dict[str, FieldMutation] = {}
        for field, value in changes.items():
            name = validate_field_name(field)
            validate_json_value(value)
            mutations[name] = FieldMutation(False, value)
        for field in delete_fields:
            name = validate_field_name(field)
            if name in mutations:
                raise ValidationError(f"field appears in changes and delete_fields: {name}")
            mutations[name] = FieldMutation(True)
        operation = cls(
            tenant=validate_identifier(tenant, "tenant"),
            document=validate_identifier(document, "document"),
            origin=validate_identifier(origin, "origin"),
            sequence=sequence,
            context=normalize_clock(dict(context)),
            mutations=mutations,
            created_ns=time.time_ns() if created_ns is None else created_ns,
        )
        operation.validate()
        operation.to_json()
        return operation

    @classmethod
    def from_dict(cls, value: Any) -> "Operation":
        if not isinstance(value, dict):
            raise ValidationError("operation must be an object")
        require_exact_keys(
            value,
            required={
                "schema",
                "op_id",
                "tenant",
                "document",
                "origin",
                "sequence",
                "context",
                "mutations",
                "created_ns",
            },
            label="operation",
        )
        if value["schema"] != 1 or isinstance(value["schema"], bool):
            raise ValidationError("unsupported operation schema")
        raw_mutations = value["mutations"]
        if not isinstance(raw_mutations, dict):
            raise ValidationError("mutations must be an object")
        mutations = {
            validate_field_name(field): FieldMutation.from_dict(mutation)
            for field, mutation in raw_mutations.items()
        }
        operation = cls(
            tenant=validate_identifier(value["tenant"], "tenant"),
            document=validate_identifier(value["document"], "document"),
            origin=validate_identifier(value["origin"], "origin"),
            sequence=value["sequence"],
            context=normalize_clock(value["context"]),
            mutations=mutations,
            created_ns=value["created_ns"],
        )
        operation.validate()
        if value["op_id"] != operation.op_id:
            raise ValidationError("op_id must equal origin:sequence")
        operation.to_json()
        return operation

    @classmethod
    def from_json(cls, data: str | bytes) -> "Operation":
        return cls.from_dict(strict_loads(data, max_bytes=MAX_OPERATION_BYTES))

    def validate(self) -> None:
        validate_identifier(self.tenant, "tenant")
        validate_identifier(self.document, "document")
        validate_identifier(self.origin, "origin")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise ValidationError("sequence must be an integer")
        if self.sequence < 1 or self.sequence > MAX_SEQUENCE:
            raise ValidationError("sequence is outside the allowed range")
        if isinstance(self.created_ns, bool) or not isinstance(self.created_ns, int):
            raise ValidationError("created_ns must be an integer")
        if self.created_ns < 0 or self.created_ns > MAX_SEQUENCE:
            raise ValidationError("created_ns is outside the allowed range")
        context = normalize_clock(self.context)
        if context.get(self.origin, 0) >= self.sequence:
            raise ValidationError("origin context must be lower than operation sequence")
        if len(self.clock) > MAX_CLOCK_ENTRIES:
            raise ValidationError(f"operation clock exceeds {MAX_CLOCK_ENTRIES} regions")
        if not self.mutations:
            raise ValidationError("operation must mutate at least one field")
        if len(self.mutations) > MAX_FIELDS_PER_OPERATION:
            raise ValidationError(
                f"operation exceeds {MAX_FIELDS_PER_OPERATION} field mutations"
            )
        for field, mutation in self.mutations.items():
            validate_field_name(field)
            if not isinstance(mutation, FieldMutation):
                raise ValidationError("mutations must contain FieldMutation values")
            if not mutation.deleted:
                validate_json_value(mutation.value)
