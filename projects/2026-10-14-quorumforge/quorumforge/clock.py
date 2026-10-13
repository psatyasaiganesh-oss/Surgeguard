"""Small immutable vector-clock implementation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from .errors import ValidationError


NODE_ID = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
MAX_COUNTER = 2**63 - 1


class ClockRelation(str, Enum):
    BEFORE = "before"
    AFTER = "after"
    EQUAL = "equal"
    CONCURRENT = "concurrent"


@dataclass(frozen=True)
class VectorClock:
    entries: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        previous = ""
        for node, counter in self.entries:
            if not isinstance(node, str) or not NODE_ID.fullmatch(node):
                raise ValidationError("clock node IDs must be lowercase letters, digits, or hyphens")
            if node <= previous:
                raise ValidationError("clock entries must be unique and sorted")
            if type(counter) is not int or not 1 <= counter <= MAX_COUNTER:
                raise ValidationError("clock counters must be positive 63-bit integers")
            previous = node

    @classmethod
    def from_dict(cls, values: Mapping[str, int] | None) -> "VectorClock":
        if values is None:
            return cls()
        if not isinstance(values, Mapping) or len(values) > 64:
            raise ValidationError("context must be an object with at most 64 clock entries")
        return cls(tuple(sorted(values.items())))

    def to_dict(self) -> dict[str, int]:
        return dict(self.entries)

    def increment(self, node: str) -> "VectorClock":
        if not isinstance(node, str) or not NODE_ID.fullmatch(node):
            raise ValidationError("invalid coordinator node ID")
        values = self.to_dict()
        current = values.get(node, 0)
        if current >= MAX_COUNTER:
            raise ValidationError("clock counter exhausted")
        values[node] = current + 1
        return VectorClock.from_dict(values)

    @classmethod
    def merge(cls, *clocks: "VectorClock") -> "VectorClock":
        values: dict[str, int] = {}
        for clock in clocks:
            if not isinstance(clock, VectorClock):
                raise TypeError("merge accepts VectorClock instances")
            for node, counter in clock.entries:
                values[node] = max(values.get(node, 0), counter)
        return cls.from_dict(values)

    def relation(self, other: "VectorClock") -> ClockRelation:
        if not isinstance(other, VectorClock):
            raise TypeError("clock comparison requires a VectorClock")
        left = self.to_dict()
        right = other.to_dict()
        nodes = left.keys() | right.keys()
        left_le = all(left.get(node, 0) <= right.get(node, 0) for node in nodes)
        right_le = all(right.get(node, 0) <= left.get(node, 0) for node in nodes)
        if left_le and right_le:
            return ClockRelation.EQUAL
        if left_le:
            return ClockRelation.BEFORE
        if right_le:
            return ClockRelation.AFTER
        return ClockRelation.CONCURRENT
