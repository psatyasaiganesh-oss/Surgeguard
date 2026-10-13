"""Fixed-bucket Merkle index for targeted replica comparison."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping

from .errors import ValidationError
from .ring import token


EMPTY_HASH = hashlib.sha256(b"").hexdigest()


@dataclass(frozen=True)
class MerkleDiff:
    divergent_buckets: tuple[int, ...]
    keys: tuple[str, ...]
    hash_nodes_compared: int
    total_keys: int


class MerkleIndex:
    def __init__(self, values: Mapping[str, str], *, buckets: int = 64):
        if type(buckets) is not int or not 2 <= buckets <= 1024 or buckets & (buckets - 1):
            raise ValidationError("Merkle bucket count must be a power of two in [2, 1024]")
        if not isinstance(values, Mapping):
            raise TypeError("values must be a mapping")
        self.values = dict(values)
        self.buckets = buckets
        grouped: list[list[tuple[str, str]]] = [[] for _ in range(buckets)]
        for key, digest in self.values.items():
            if not isinstance(key, str) or not isinstance(digest, str) or len(digest) != 64:
                raise ValidationError("Merkle values must map string keys to SHA-256 hex digests")
            grouped[token(key) % buckets].append((key, digest))
        self._keys_by_bucket = tuple(tuple(key for key, _ in sorted(group)) for group in grouped)
        leaves = []
        for group in grouped:
            payload = b"".join(
                key.encode() + b"\0" + digest.encode("ascii") + b"\0"
                for key, digest in sorted(group)
            )
            leaves.append(hashlib.sha256(payload).hexdigest())
        levels = [tuple(leaves)]
        current = leaves
        while len(current) > 1:
            current = [
                hashlib.sha256((current[index] + current[index + 1]).encode("ascii")).hexdigest()
                for index in range(0, len(current), 2)
            ]
            levels.append(tuple(current))
        self.levels = tuple(levels)

    @property
    def root(self) -> str:
        return self.levels[-1][0] if self.levels else EMPTY_HASH

    def diff(self, other: "MerkleIndex") -> MerkleDiff:
        if not isinstance(other, MerkleIndex) or self.buckets != other.buckets:
            raise ValidationError("Merkle indexes must use the same bucket count")
        divergent: list[int] = []
        compared = 0

        def visit(level: int, position: int) -> None:
            nonlocal compared
            compared += 1
            if self.levels[level][position] == other.levels[level][position]:
                return
            if level == 0:
                divergent.append(position)
                return
            visit(level - 1, position * 2)
            visit(level - 1, position * 2 + 1)

        visit(len(self.levels) - 1, 0)
        keys: set[str] = set()
        for bucket in divergent:
            keys.update(self._keys_by_bucket[bucket])
            keys.update(other._keys_by_bucket[bucket])
        return MerkleDiff(
            divergent_buckets=tuple(divergent),
            keys=tuple(sorted(keys)),
            hash_nodes_compared=compared,
            total_keys=len(set(self.values) | set(other.values)),
        )
