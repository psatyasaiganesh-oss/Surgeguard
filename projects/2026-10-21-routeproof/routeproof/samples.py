"""Deterministic synthetic topology and change plans used by tests and demo."""

from __future__ import annotations

from typing import Any


def sample_topology(version: int = 1) -> dict[str, Any]:
    nodes = [
        {"id": "core", "kind": "core"},
        {"id": "dc-a", "kind": "datacenter"},
        {"id": "dc-b", "kind": "datacenter"},
        {"id": "dc-c", "kind": "datacenter"},
        {"id": "dns-a", "kind": "dns"},
        {"id": "dns-b", "kind": "dns"},
        {"id": "noc", "kind": "noc"},
    ]
    links: list[dict[str, Any]] = []
    for dc in ("dc-a", "dc-b", "dc-c"):
        for index in range(1, 4):
            links.append({
                "id": f"{dc}-backbone-{index}",
                "a": dc,
                "b": "core",
                "plane": "backbone",
                "up": True,
                "failure_domain": f"{dc}-fiber-{index}",
            })
    for dns in ("dns-a", "dns-b"):
        for index in range(1, 3):
            links.append({
                "id": f"{dns}-backbone-{index}",
                "a": dns,
                "b": "core",
                "plane": "backbone",
                "up": True,
                "failure_domain": f"{dns}-fiber-{index}",
            })
    for target in ("core", "dc-a", "dc-b", "dc-c"):
        for index in range(1, 3):
            links.append({
                "id": f"noc-{target}-{index}",
                "a": "noc",
                "b": target,
                "plane": "management",
                "up": True,
                "failure_domain": f"mgmt-{target}-{index}",
            })
    return {
        "version": version,
        "nodes": nodes,
        "links": links,
        "requirements": {
            "core": "core",
            "datacenters": ["dc-a", "dc-b", "dc-c"],
            "dns_nodes": ["dns-a", "dns-b"],
            "operator": "noc",
            "management_targets": ["core", "dc-a", "dc-b", "dc-c"],
            "min_dns_advertisers": 1,
            "max_disconnected_datacenters": 0,
            "max_disconnected_management": 0,
            "require_single_failure_tolerance": True,
        },
    }


def safe_plan(version: int = 1, plan_id: str = "safe-maintenance") -> dict[str, Any]:
    return {
        "plan_id": plan_id,
        "reason": "Replace one backbone circuit while redundant paths remain available.",
        "expected_topology_version": version,
        "steps": [{"action": "set_link", "link_id": "dc-a-backbone-1", "up": False}],
    }


def unsafe_backbone_plan(version: int = 1, plan_id: str = "unsafe-backbone") -> dict[str, Any]:
    return {
        "plan_id": plan_id,
        "reason": "Remove every backbone path for one data center in one maintenance plan.",
        "expected_topology_version": version,
        "steps": [
            {"action": "set_link", "link_id": f"dc-a-backbone-{index}", "up": False}
            for index in range(1, 4)
        ],
    }


def unsafe_dns_plan(version: int = 1, plan_id: str = "unsafe-dns") -> dict[str, Any]:
    return {
        "plan_id": plan_id,
        "reason": "Remove all DNS-to-core paths and trigger route withdrawal.",
        "expected_topology_version": version,
        "steps": [
            {"action": "set_link", "link_id": f"{dns}-backbone-{index}", "up": False}
            for dns in ("dns-a", "dns-b")
            for index in range(1, 3)
        ],
    }
