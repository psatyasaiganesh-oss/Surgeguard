"""Deterministic experiments for availability, conflicts, and repair."""

from __future__ import annotations

import itertools
import json
import math
import platform
import random
import time
from datetime import datetime, timezone

from .cluster import ClusterConfig, QuorumCluster
from .ring import ConsistentHashRing


def _coefficient_of_variation(counts: dict[str, int]) -> float:
    values = list(counts.values())
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values)) / mean


def availability_and_repair(key_count: int, seed: int) -> tuple[dict, dict]:
    node_ids = tuple(f"n{index}" for index in range(1, 8))
    cluster = QuorumCluster(
        node_ids,
        ClusterConfig(replication_factor=3, read_quorum=2, write_quorum=2, virtual_nodes=32, merkle_buckets=64),
    )
    failed = ("n1", "n2")
    for node in failed:
        cluster.fail_node(node)

    strict_successes = 0
    offered = [f"cart:{index:05d}" for index in range(key_count)]
    started = time.perf_counter()
    for index, key in enumerate(offered):
        intended = cluster.intended_replicas(key)
        if sum(cluster.nodes[node].alive for node in intended) >= cluster.config.write_quorum:
            strict_successes += 1
        cluster.put(key, json.dumps({"items": [f"sku-{index:05d}"]}, separators=(",", ":")), coordinator="n3")
    write_seconds = time.perf_counter() - started
    outage_snapshot = cluster.snapshot()
    for node in failed:
        cluster.recover_node(node)
    handoff = cluster.handoff()
    post_handoff = cluster.audit()

    pairs = list(itertools.combinations(node_ids, 2))
    pair = max(pairs, key=lambda item: len(cluster.shared_replica_keys(*item)))
    shared = sorted(cluster.shared_replica_keys(*pair))
    if len(shared) < 12:
        raise AssertionError("demo did not produce enough shared replica keys")
    rng = random.Random(seed)
    damaged = tuple(sorted(rng.sample(shared, 12)))
    for key in damaged:
        cluster.unsafe_drop(pair[1], key)
    repair = cluster.anti_entropy_pair(*pair)
    repair["faults_injected"] = len(damaged)
    repair["full_scan_keys"] = len(shared)

    availability = {
        "keys_offered": key_count,
        "failed_nodes": list(failed),
        "strict_intended_quorum_successes": strict_successes,
        "strict_intended_quorum_failures": key_count - strict_successes,
        "sloppy_quorum_successes": cluster.metrics["writes_succeeded"],
        "sloppy_quorum_failures": cluster.metrics["writes_failed"],
        "hints_created": outage_snapshot["metrics"]["hints_created"],
        "handoff": handoff,
        "pending_hints_after_handoff": post_handoff["pending_hints"],
        "under_replicated_after_handoff": post_handoff["under_replicated_keys"],
        "write_elapsed_seconds": round(write_seconds, 6),
    }
    return availability, repair


def conflict_experiment() -> dict:
    node_ids = ("c1", "c2", "c3", "c4", "c5", "c6")
    group_a = ("c1", "c3", "c5")
    group_b = ("c2", "c4", "c6")
    cluster = QuorumCluster(
        node_ids,
        ClusterConfig(replication_factor=3, read_quorum=1, write_quorum=1, virtual_nodes=32),
    )
    key = next(
        f"cart:conflict-{index}"
        for index in range(10_000)
        if set(cluster.intended_replicas(f"cart:conflict-{index}")) & set(group_a)
        and set(cluster.intended_replicas(f"cart:conflict-{index}")) & set(group_b)
    )
    initial_coordinator = cluster.intended_replicas(key)[0]
    initial = cluster.put(key, '{"items":[]}', coordinator=initial_coordinator)
    base_context = cluster.get(key, coordinator=initial_coordinator).context
    cluster.partition([group_a, group_b])
    coordinator_a = next(node for node in cluster.intended_replicas(key) if node in group_a)
    coordinator_b = next(node for node in cluster.intended_replicas(key) if node in group_b)
    left = cluster.put(key, '{"items":["book"]}', coordinator=coordinator_a, context=base_context)
    right = cluster.put(key, '{"items":["pen"]}', coordinator=coordinator_b, context=base_context)
    cluster.heal_network()
    conflicted = cluster.get(key, coordinator=initial_coordinator)
    values = sorted(version.value for version in conflicted.versions if not version.tombstone)
    resolved = cluster.put(
        key,
        '{"items":["book","pen"]}',
        coordinator=initial_coordinator,
        context=conflicted.context,
    )
    final = cluster.get(key, coordinator=initial_coordinator)
    return {
        "key": key,
        "initial_clock": initial.clock.to_dict(),
        "branch_clocks": [left.clock.to_dict(), right.clock.to_dict()],
        "siblings_before_resolution": len(conflicted.versions),
        "sibling_values": values,
        "merged_context": conflicted.context.to_dict(),
        "resolution_clock": resolved.clock.to_dict(),
        "siblings_after_resolution": len(final.versions),
        "resolved_value": None if not final.versions else final.versions[0].value,
        "naive_last_write_wins_updates_lost": 1,
        "quorumforge_updates_lost_after_resolution": 0,
    }


def distribution_experiment(key_count: int) -> dict:
    nodes = tuple(f"d{index}" for index in range(1, 9))
    keys = tuple(f"session:{index:06d}" for index in range(key_count))
    single = ConsistentHashRing(nodes, virtual_nodes=1).primary_counts(keys)
    virtual = ConsistentHashRing(nodes, virtual_nodes=64).primary_counts(keys)
    return {
        "keys": key_count,
        "nodes": len(nodes),
        "single_token_counts": single,
        "virtual_node_counts": virtual,
        "single_token_cv": round(_coefficient_of_variation(single), 6),
        "virtual_node_cv": round(_coefficient_of_variation(virtual), 6),
    }


def run_demo(key_count: int = 600, *, seed: int = 14) -> dict:
    if type(key_count) is not int or not 100 <= key_count <= 20_000:
        raise ValueError("key_count must be an integer in [100, 20000]")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    availability, repair = availability_and_repair(key_count, seed)
    conflicts = conflict_experiment()
    distribution = distribution_experiment(max(5_000, key_count * 10))
    checks = {
        "strict_quorum_lost_availability": availability["strict_intended_quorum_failures"] > 0,
        "sloppy_quorum_accepted_every_write": availability["sloppy_quorum_successes"] == key_count,
        "hinted_handoff_drained": availability["pending_hints_after_handoff"] == 0,
        "replication_restored": availability["under_replicated_after_handoff"] == 0,
        "merkle_found_every_injected_fault": repair["keys_repaired"] == repair["faults_injected"] == 12,
        "merkle_examined_less_than_full_scan": repair["keys_examined"] < repair["full_scan_keys"],
        "merkle_roots_converged": repair["root_equal_after"],
        "concurrent_branches_preserved": conflicts["siblings_before_resolution"] == 2,
        "client_resolution_collapsed_siblings": conflicts["siblings_after_resolution"] == 1,
        "resolved_cart_preserved_both_updates": conflicts["resolved_value"] == '{"items":["book","pen"]}',
        "virtual_nodes_improved_balance": distribution["virtual_node_cv"] < distribution["single_token_cv"],
    }
    if not all(checks.values()):
        raise AssertionError(f"demo invariant failed: {checks}")
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(),
        "platform": platform.system(),
        "scope": "Deterministic in-process simulation; not Amazon traffic or a production benchmark",
        "seed": seed,
        "sources": [
            "https://www.amazon.science/publications/dynamo-amazons-highly-available-key-value-store",
            "https://cdn.amazon.science/ac/1d/eb50c4064c538c8ac440ce6a1d91/dynamo-amazons-highly-available-key-value-store.pdf",
        ],
        "availability_and_handoff": availability,
        "merkle_repair": repair,
        "conflict_resolution": conflicts,
        "partition_balance": distribution,
        "invariants": checks,
    }
