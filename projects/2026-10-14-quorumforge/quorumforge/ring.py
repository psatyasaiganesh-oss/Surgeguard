"""Deterministic consistent-hash ring with virtual nodes."""

from __future__ import annotations

import bisect
import hashlib

from .clock import NODE_ID
from .errors import ValidationError
from .model import validate_key


HASH_SPACE = 2**64


def token(value: str) -> int:
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], "big")


class ConsistentHashRing:
    def __init__(self, nodes: list[str] | tuple[str, ...], *, virtual_nodes: int = 32):
        if not isinstance(nodes, (list, tuple)) or not 3 <= len(nodes) <= 64:
            raise ValidationError("ring requires between 3 and 64 nodes")
        if len(set(nodes)) != len(nodes) or any(not isinstance(node, str) or not NODE_ID.fullmatch(node) for node in nodes):
            raise ValidationError("ring node IDs must be unique and valid")
        if type(virtual_nodes) is not int or not 1 <= virtual_nodes <= 256:
            raise ValidationError("virtual_nodes must be an integer in [1, 256]")
        self.nodes = tuple(sorted(nodes))
        self.virtual_nodes = virtual_nodes
        entries = [(token(f"{node}#{index}"), node) for node in self.nodes for index in range(virtual_nodes)]
        entries.sort()
        self._entries = tuple(entries)
        self._tokens = tuple(item[0] for item in entries)

    def preference_list(self, key: str, count: int | None = None) -> tuple[str, ...]:
        validate_key(key)
        wanted = len(self.nodes) if count is None else count
        if type(wanted) is not int or not 1 <= wanted <= len(self.nodes):
            raise ValidationError("preference count must be between 1 and the ring size")
        index = bisect.bisect_left(self._tokens, token(key))
        selected: list[str] = []
        seen: set[str] = set()
        for offset in range(len(self._entries)):
            node = self._entries[(index + offset) % len(self._entries)][1]
            if node not in seen:
                selected.append(node)
                seen.add(node)
                if len(selected) == wanted:
                    return tuple(selected)
        raise RuntimeError("ring could not produce a complete preference list")

    def primary_counts(self, keys: list[str] | tuple[str, ...]) -> dict[str, int]:
        counts = {node: 0 for node in self.nodes}
        for key in keys:
            counts[self.preference_list(key, 1)[0]] += 1
        return counts
