"""Immutable values shared by storage nodes and coordinators."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from .clock import VectorClock
from .errors import ValidationError


MAX_KEY_BYTES = 128
MAX_VALUE_BYTES = 8 * 1024
ALLOWED_KEY = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:_./-")


def validate_key(key: str) -> str:
    if not isinstance(key, str):
        raise ValidationError("key must be a string")
    encoded = key.encode("utf-8")
    if not 1 <= len(encoded) <= MAX_KEY_BYTES or any(character not in ALLOWED_KEY for character in key):
        raise ValidationError("key must be 1-128 bytes using letters, digits, ':', '_', '.', '/', or '-'")
    return key


def validate_value(value: str) -> str:
    if not isinstance(value, str):
        raise ValidationError("value must be a string")
    if len(value.encode("utf-8")) > MAX_VALUE_BYTES:
        raise ValidationError(f"value must not exceed {MAX_VALUE_BYTES} UTF-8 bytes")
    return value


@dataclass(frozen=True)
class Version:
    value: str
    clock: VectorClock
    coordinator: str
    tombstone: bool = False

    def __post_init__(self) -> None:
        validate_value(self.value)
        if not isinstance(self.clock, VectorClock):
            raise TypeError("clock must be a VectorClock")
        if not isinstance(self.coordinator, str):
            raise ValidationError("coordinator must be a string")
        if type(self.tombstone) is not bool:
            raise ValidationError("tombstone must be a boolean")
        if self.tombstone and self.value:
            raise ValidationError("tombstones cannot contain a value")

    @property
    def digest(self) -> str:
        canonical = json.dumps(
            {
                "clock": self.clock.to_dict(),
                "coordinator": self.coordinator,
                "tombstone": self.tombstone,
                "value": self.value,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

    def to_dict(self) -> dict:
        return {
            "value": None if self.tombstone else self.value,
            "clock": self.clock.to_dict(),
            "coordinator": self.coordinator,
            "tombstone": self.tombstone,
            "digest": self.digest,
        }
