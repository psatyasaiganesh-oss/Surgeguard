"""Failure-aware quorum coordinator for the local Dynamo-inspired lab."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .clock import VectorClock
from .errors import UnavailableError, ValidationError
from .merkle import MerkleIndex
from .model import Version, validate_key, validate_value
from .node import StorageNode, merge_versions
from .ring import ConsistentHashRing


@dataclass(frozen=True)
class ClusterConfig:
    replication_factor: int = 3
    read_quorum: int = 2
    write_quorum: int = 2
    virtual_nodes: int = 32
    merkle_buckets: int = 64

    def __post_init__(self) -> None:
        for name in ("replication_factor", "read_quorum", "write_quorum", "virtual_nodes", "merkle_buckets"):
            if type(getattr(self, name)) is not int:
                raise ValidationError(f"{name} must be an integer")
        if not 1 <= self.replication_factor <= 16:
            raise ValidationError("replication_factor must be in [1, 16]")
        if not 1 <= self.read_quorum <= self.replication_factor:
            raise ValidationError("read_quorum must be in [1, replication_factor]")
        if not 1 <= self.write_quorum <= self.replication_factor:
            raise ValidationError("write_quorum must be in [1, replication_factor]")
        if not 1 <= self.virtual_nodes <= 256:
            raise ValidationError("virtual_nodes must be in [1, 256]")
        if not 2 <= self.merkle_buckets <= 1024 or self.merkle_buckets & (self.merkle_buckets - 1):
            raise ValidationError("merkle_buckets must be a power of two in [2, 1024]")


@dataclass(frozen=True)
class WriteResult:
    key: str
    clock: VectorClock
    acknowledgements: int
    replicas: tuple[str, ...]
    hinted_replicas: tuple[tuple[str, str], ...]
    tombstone: bool

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "clock": self.clock.to_dict(),
            "acknowledgements": self.acknowledgements,
            "replicas": list(self.replicas),
            "hinted_replicas": [
                {"holder": holder, "intended": intended} for holder, intended in self.hinted_replicas
            ],
            "tombstone": self.tombstone,
        }


@dataclass(frozen=True)
class ReadResult:
    key: str
    versions: tuple[Version, ...]
    context: VectorClock
    responses: int
    replicas: tuple[str, ...]
    repairs: int

    @property
    def found(self) -> bool:
        return any(not version.tombstone for version in self.versions)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "found": self.found,
            "siblings": [version.to_dict() for version in self.versions],
            "context": self.context.to_dict(),
            "responses": self.responses,
            "replicas": list(self.replicas),
            "read_repairs": self.repairs,
        }


class QuorumCluster:
    def __init__(self, nodes: list[str] | tuple[str, ...], config: ClusterConfig | None = None):
        self.config = config or ClusterConfig()
        if len(nodes) < self.config.replication_factor:
            raise ValidationError("node count must be at least the replication factor")
        self.ring = ConsistentHashRing(nodes, virtual_nodes=self.config.virtual_nodes)
        self.nodes = {node: StorageNode(node) for node in self.ring.nodes}
        self._blocked: set[frozenset[str]] = set()
        self.metrics = {
            "writes_succeeded": 0,
            "writes_failed": 0,
            "reads_succeeded": 0,
            "reads_failed": 0,
            "read_repairs": 0,
            "hints_created": 0,
            "hints_delivered": 0,
        }

    def _node(self, node_id: str) -> StorageNode:
        try:
            return self.nodes[node_id]
        except (KeyError, TypeError) as exc:
            raise ValidationError(f"unknown node: {node_id}") from exc

    def fail_node(self, node_id: str) -> None:
        self._node(node_id).alive = False

    def recover_node(self, node_id: str) -> None:
        self._node(node_id).alive = True

    def partition(self, groups: list[list[str] | tuple[str, ...]]) -> None:
        flattened = [node for group in groups for node in group]
        if len(groups) < 2 or len(flattened) != len(set(flattened)) or set(flattened) != set(self.nodes):
            raise ValidationError("partition groups must contain every node exactly once")
        self._blocked.clear()
        for index, left in enumerate(groups):
            for right in groups[index + 1 :]:
                for node_a in left:
                    for node_b in right:
                        self._blocked.add(frozenset((node_a, node_b)))

    def heal_network(self) -> None:
        self._blocked.clear()

    def reachable(self, source: str, target: str) -> bool:
        source_node = self._node(source)
        target_node = self._node(target)
        return source_node.alive and target_node.alive and frozenset((source, target)) not in self._blocked

    def intended_replicas(self, key: str) -> tuple[str, ...]:
        return self.ring.preference_list(validate_key(key), self.config.replication_factor)

    def _targets(self, key: str, coordinator: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        self._node(coordinator)
        if not self.nodes[coordinator].alive:
            raise UnavailableError(f"coordinator {coordinator} is unavailable")
        preference = self.ring.preference_list(key)
        intended = preference[: self.config.replication_factor]
        actual = tuple(
            node for node in preference if self.reachable(coordinator, node)
        )[: self.config.replication_factor]
        return intended, actual

    def _context(self, value: VectorClock | Mapping[str, int] | None) -> VectorClock:
        if isinstance(value, VectorClock):
            clock = value
        else:
            clock = VectorClock.from_dict(value)
        unknown = set(clock.to_dict()) - set(self.nodes)
        if unknown:
            raise ValidationError("context contains nodes outside this cluster")
        return clock

    def put(
        self,
        key: str,
        value: str,
        *,
        coordinator: str,
        context: VectorClock | Mapping[str, int] | None = None,
    ) -> WriteResult:
        return self._write(key, validate_value(value), coordinator=coordinator, context=context, tombstone=False)

    def delete(
        self,
        key: str,
        *,
        coordinator: str,
        context: VectorClock | Mapping[str, int] | None = None,
    ) -> WriteResult:
        return self._write(key, "", coordinator=coordinator, context=context, tombstone=True)

    def _write(
        self,
        key: str,
        value: str,
        *,
        coordinator: str,
        context: VectorClock | Mapping[str, int] | None,
        tombstone: bool,
    ) -> WriteResult:
        key = validate_key(key)
        base = self._context(context)
        intended, actual = self._targets(key, coordinator)
        clock = base.increment(coordinator)
        version = Version(value=value, clock=clock, coordinator=coordinator, tombstone=tombstone)
        missing = [node for node in intended if node not in actual]
        hinted: list[tuple[str, str]] = []
        for target in actual:
            if target in intended:
                self.nodes[target].store(key, version)
            else:
                hint_target = missing.pop(0)
                self.nodes[target].store_hint(hint_target, key, version)
                hinted.append((target, hint_target))
                self.metrics["hints_created"] += 1
        if len(actual) < self.config.write_quorum:
            self.metrics["writes_failed"] += 1
            raise UnavailableError(
                f"write quorum unavailable: received {len(actual)} of {self.config.write_quorum} acknowledgements"
            )
        self.metrics["writes_succeeded"] += 1
        return WriteResult(key, clock, len(actual), actual, tuple(hinted), tombstone)

    def get(self, key: str, *, coordinator: str) -> ReadResult:
        key = validate_key(key)
        intended, actual = self._targets(key, coordinator)
        if len(actual) < self.config.read_quorum:
            self.metrics["reads_failed"] += 1
            raise UnavailableError(
                f"read quorum unavailable: received {len(actual)} of {self.config.read_quorum} responses"
            )
        versions = merge_versions(*(self.nodes[node].read(key) for node in actual))
        repairs = 0
        if versions:
            for node in actual:
                if node not in intended:
                    continue
                before = self.nodes[node].normal_read(key)
                after = merge_versions(before, versions)
                if after != before:
                    self.nodes[node].replace(key, after)
                    repairs += 1
        context = VectorClock.merge(*(version.clock for version in versions))
        self.metrics["reads_succeeded"] += 1
        self.metrics["read_repairs"] += repairs
        return ReadResult(key, versions, context, len(actual), actual, repairs)

    def handoff(self) -> dict[str, int]:
        delivered_keys = 0
        delivered_versions = 0
        for holder_id in sorted(self.nodes):
            holder = self.nodes[holder_id]
            if not holder.alive:
                continue
            for target_id, key, versions in list(holder.hint_items()):
                if not self.reachable(holder_id, target_id):
                    continue
                for version in versions:
                    self.nodes[target_id].store(key, version)
                    delivered_versions += 1
                holder.delete_hint(target_id, key)
                delivered_keys += 1
        self.metrics["hints_delivered"] += delivered_keys
        return {"keys_delivered": delivered_keys, "versions_delivered": delivered_versions}

    def all_keys(self) -> set[str]:
        result: set[str] = set()
        for node in self.nodes.values():
            result.update(node.keys())
        return result

    def shared_replica_keys(self, node_a: str, node_b: str) -> set[str]:
        self._node(node_a)
        self._node(node_b)
        return {
            key for key in self.all_keys()
            if node_a in self.intended_replicas(key) and node_b in self.intended_replicas(key)
        }

    def anti_entropy_pair(self, node_a: str, node_b: str) -> dict:
        if node_a == node_b:
            raise ValidationError("anti-entropy requires two different nodes")
        if not self.reachable(node_a, node_b):
            raise UnavailableError("replicas cannot communicate for anti-entropy")
        shared = self.shared_replica_keys(node_a, node_b)
        left = MerkleIndex(self.nodes[node_a].snapshot_digests(shared), buckets=self.config.merkle_buckets)
        right = MerkleIndex(self.nodes[node_b].snapshot_digests(shared), buckets=self.config.merkle_buckets)
        difference = left.diff(right)
        repaired = 0
        versions_copied = 0
        for key in difference.keys:
            before_a = self.nodes[node_a].normal_read(key)
            before_b = self.nodes[node_b].normal_read(key)
            merged = merge_versions(before_a, before_b)
            if before_a != merged:
                versions_copied += len({item.digest for item in merged} - {item.digest for item in before_a})
                self.nodes[node_a].replace(key, merged)
            if before_b != merged:
                versions_copied += len({item.digest for item in merged} - {item.digest for item in before_b})
                self.nodes[node_b].replace(key, merged)
            if before_a != before_b:
                repaired += 1
        after_a = MerkleIndex(self.nodes[node_a].snapshot_digests(shared), buckets=self.config.merkle_buckets)
        after_b = MerkleIndex(self.nodes[node_b].snapshot_digests(shared), buckets=self.config.merkle_buckets)
        return {
            "nodes": [node_a, node_b],
            "shared_keys": len(shared),
            "root_equal_before": left.root == right.root,
            "root_equal_after": after_a.root == after_b.root,
            "divergent_buckets": len(difference.divergent_buckets),
            "keys_examined": len(difference.keys),
            "hash_nodes_compared": difference.hash_nodes_compared,
            "keys_repaired": repaired,
            "versions_copied": versions_copied,
        }

    def unsafe_drop(self, node_id: str, key: str) -> None:
        self._node(node_id).unsafe_drop(key)

    def audit(self) -> dict:
        divergent = 0
        under_replicated = 0
        keys = self.all_keys()
        for key in keys:
            intended = self.intended_replicas(key)
            digests = []
            copies = 0
            for node in intended:
                versions = self.nodes[node].normal_read(key)
                digest_set = tuple(item.digest for item in versions)
                digests.append(digest_set)
                copies += bool(versions)
            if len(set(digests)) > 1:
                divergent += 1
            if copies < self.config.replication_factor:
                under_replicated += 1
        return {
            "keys": len(keys),
            "divergent_keys": divergent,
            "under_replicated_keys": under_replicated,
            "pending_hints": sum(node.hint_count() for node in self.nodes.values()),
        }

    def snapshot(self) -> dict:
        return {
            "config": {
                "N": self.config.replication_factor,
                "R": self.config.read_quorum,
                "W": self.config.write_quorum,
                "virtual_nodes": self.config.virtual_nodes,
                "merkle_buckets": self.config.merkle_buckets,
            },
            "nodes": {node_id: {"alive": node.alive, "keys": len(node.normal_keys()), "hints": node.hint_count()} for node_id, node in self.nodes.items()},
            "network_blocks": len(self._blocked),
            "metrics": dict(self.metrics),
            "audit": self.audit(),
        }
