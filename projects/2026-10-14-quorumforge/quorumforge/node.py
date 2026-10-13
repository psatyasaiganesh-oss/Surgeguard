"""Replica storage and hinted-handoff state."""

from __future__ import annotations

import hashlib

from .clock import ClockRelation
from .errors import ConflictError
from .model import Version, validate_key


def merge_versions(*groups: tuple[Version, ...] | list[Version]) -> tuple[Version, ...]:
    result: list[Version] = []
    for version in (item for group in groups for item in group):
        obsolete = False
        survivors: list[Version] = []
        for current in result:
            relation = version.clock.relation(current.clock)
            if relation is ClockRelation.EQUAL:
                if version.digest != current.digest:
                    raise ConflictError("equal vector clocks contain different values")
                obsolete = True
                survivors.append(current)
            elif relation is ClockRelation.BEFORE:
                obsolete = True
                survivors.append(current)
            elif relation is ClockRelation.AFTER:
                continue
            else:
                survivors.append(current)
        result = survivors
        if not obsolete:
            result.append(version)
    return tuple(sorted(result, key=lambda item: item.digest))


class StorageNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.alive = True
        self._data: dict[str, tuple[Version, ...]] = {}
        self._hints: dict[str, dict[str, tuple[Version, ...]]] = {}

    def store(self, key: str, version: Version) -> bool:
        validate_key(key)
        before = self._data.get(key, ())
        after = merge_versions(before, (version,))
        self._data[key] = after
        return after != before

    def store_hint(self, target: str, key: str, version: Version) -> bool:
        validate_key(key)
        target_hints = self._hints.setdefault(target, {})
        before = target_hints.get(key, ())
        after = merge_versions(before, (version,))
        target_hints[key] = after
        return after != before

    def read(self, key: str) -> tuple[Version, ...]:
        validate_key(key)
        groups: list[tuple[Version, ...]] = [self._data.get(key, ())]
        groups.extend(target.get(key, ()) for target in self._hints.values())
        return merge_versions(*groups)

    def normal_read(self, key: str) -> tuple[Version, ...]:
        return self._data.get(validate_key(key), ())

    def replace(self, key: str, versions: tuple[Version, ...]) -> None:
        validate_key(key)
        merged = merge_versions(versions)
        if merged:
            self._data[key] = merged
        else:
            self._data.pop(key, None)

    def hint_items(self):
        for target in sorted(self._hints):
            for key in sorted(self._hints[target]):
                yield target, key, self._hints[target][key]

    def delete_hint(self, target: str, key: str) -> None:
        target_hints = self._hints.get(target)
        if target_hints is None:
            return
        target_hints.pop(key, None)
        if not target_hints:
            self._hints.pop(target, None)

    def keys(self) -> set[str]:
        result = set(self._data)
        for target in self._hints.values():
            result.update(target)
        return result

    def normal_keys(self) -> set[str]:
        return set(self._data)

    def hint_count(self) -> int:
        return sum(len(keys) for keys in self._hints.values())

    def snapshot_digests(self, keys: set[str] | None = None) -> dict[str, str]:
        selected = self.normal_keys() if keys is None else keys
        result = {}
        for key in selected:
            versions = self._data.get(key, ())
            if versions:
                joined = "".join(version.digest for version in versions).encode("ascii")
                result[key] = hashlib.sha256(joined).hexdigest()
        return result

    def unsafe_drop(self, key: str) -> None:
        """Fault injection used only by tests and the deterministic demo."""
        self._data.pop(validate_key(key), None)
