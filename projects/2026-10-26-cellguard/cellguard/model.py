"""Validated models and deterministic serialization."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import ValidationError


ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_JSON_BYTES = 16_384
MAX_TIMESTAMP_NS = 9_223_372_036_854_775_807


def validate_id(value: str, field: str) -> str:
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValidationError(
            f"{field} must match {ID_RE.pattern} and be at most 96 characters"
        )
    return value


def validate_integer(
    value: int, field: str, minimum: int, maximum: int
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} must be an integer")
    if value < minimum or value > maximum:
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def validate_timestamp(value: int, field: str = "timestamp") -> int:
    return validate_integer(value, field, 0, MAX_TIMESTAMP_NS)


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
        raise ValidationError("payload must be finite JSON") from exc
    if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
        raise ValidationError(f"JSON exceeds {MAX_JSON_BYTES} UTF-8 bytes")
    return encoded


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Cell:
    cell_id: str
    zone: str
    capacity: int = 1000

    def validated(self) -> "Cell":
        validate_id(self.cell_id, "cell_id")
        validate_id(self.zone, "zone")
        validate_integer(self.capacity, "capacity", 1, 1_000_000)
        return self

    def as_dict(self) -> dict[str, Any]:
        return {"capacity": self.capacity, "cell_id": self.cell_id, "zone": self.zone}


def validate_cells(cells: Iterable[Cell]) -> tuple[Cell, ...]:
    result = tuple(cell.validated() for cell in cells)
    if len(result) < 2 or len(result) > 256:
        raise ValidationError("topology requires between 2 and 256 cells")
    ids = [cell.cell_id for cell in result]
    if len(ids) != len(set(ids)):
        raise ValidationError("cell identifiers must be unique")
    return tuple(sorted(result, key=lambda cell: cell.cell_id))


@dataclass(frozen=True)
class Assignment:
    tenant: str
    epoch: str
    cells: tuple[str, ...]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "cells": list(self.cells),
            "digest": self.digest,
            "epoch": self.epoch,
            "tenant": self.tenant,
        }


@dataclass(frozen=True)
class SubmitResult:
    job_id: str
    tenant: str
    cell_id: str
    state: str
    replayed: bool
    assignment: Assignment

    def as_dict(self) -> dict[str, Any]:
        return {
            "assignment": self.assignment.as_dict(),
            "cell_id": self.cell_id,
            "job_id": self.job_id,
            "replayed": self.replayed,
            "state": self.state,
            "tenant": self.tenant,
        }


@dataclass(frozen=True)
class JobClaim:
    job_id: str
    tenant: str
    cell_id: str
    payload: Any
    priority: int
    attempt: int
    fencing_token: int
    lease_token: str
    lease_until_ns: int

    def as_dict(self, *, include_capability: bool = True) -> dict[str, Any]:
        result = {
            "attempt": self.attempt,
            "cell_id": self.cell_id,
            "fencing_token": self.fencing_token,
            "job_id": self.job_id,
            "lease_until_ns": self.lease_until_ns,
            "payload": self.payload,
            "priority": self.priority,
            "tenant": self.tenant,
        }
        if include_capability:
            result["lease_token"] = self.lease_token
        return result
