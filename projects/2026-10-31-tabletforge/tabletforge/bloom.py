from __future__ import annotations

import base64
import hashlib
import math
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import ValidationError
from .model import require_closed_object, require_int


@dataclass(frozen=True)
class BloomFilter:
    bit_count: int
    hash_count: int
    bits: bytes

    @staticmethod
    def _positions(key: str, bit_count: int, hash_count: int) -> list[int]:
        encoded = key.encode()
        first = int.from_bytes(hashlib.sha256(b"a" + encoded).digest()[:8], "big")
        second = int.from_bytes(hashlib.sha256(b"b" + encoded).digest()[:8], "big") | 1
        return [(first + index * second) % bit_count for index in range(hash_count)]

    @classmethod
    def build(cls, keys: Iterable[str], *, bits_per_key: int = 10) -> "BloomFilter":
        unique = sorted(set(keys))
        bit_count = max(64, len(unique) * bits_per_key)
        bit_count = int(math.ceil(bit_count / 8) * 8)
        hash_count = max(1, min(12, round(bits_per_key * math.log(2))))
        mutable = bytearray(bit_count // 8)
        for key in unique:
            for position in cls._positions(key, bit_count, hash_count):
                mutable[position // 8] |= 1 << (position % 8)
        return cls(bit_count, hash_count, bytes(mutable))

    def might_contain(self, key: str) -> bool:
        return all(
            self.bits[position // 8] & (1 << (position % 8))
            for position in self._positions(key, self.bit_count, self.hash_count)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "bit_count": self.bit_count,
            "hash_count": self.hash_count,
            "bits_base64": base64.b64encode(self.bits).decode(),
        }

    @classmethod
    def from_value(cls, value: Any) -> "BloomFilter":
        body = require_closed_object(
            value, "bloom", {"bit_count", "hash_count", "bits_base64"}
        )
        bit_count = require_int(body["bit_count"], "bit_count", minimum=8, maximum=100_000_000)
        if bit_count % 8:
            raise ValidationError("bit_count must be divisible by 8")
        hash_count = require_int(body["hash_count"], "hash_count", minimum=1, maximum=32)
        if not isinstance(body["bits_base64"], str):
            raise ValidationError("bits_base64 must be a string")
        try:
            bits = base64.b64decode(body["bits_base64"], validate=True)
        except ValueError as exc:
            raise ValidationError("bits_base64 is invalid") from exc
        if len(bits) * 8 != bit_count:
            raise ValidationError("Bloom bit payload has the wrong length")
        return cls(bit_count, hash_count, bits)
