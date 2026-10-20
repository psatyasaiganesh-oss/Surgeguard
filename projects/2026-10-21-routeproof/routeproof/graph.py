"""Dependency-aware topology simulation and fail-closed change analysis."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .model import digest, parse_plan, parse_topology


class AnalysisFailure(RuntimeError):
    """Raised when the analyzer cannot establish a definitive answer."""


def _reachable(topology: dict[str, Any], start: str, plane: str) -> set[str]:
    adjacency: dict[str, set[str]] = {node["id"]: set() for node in topology["nodes"]}
    for link in topology["links"]:
        if link["up"] and link["plane"] == plane:
            adjacency[link["a"]].add(link["b"])
            adjacency[link["b"]].add(link["a"])
    visited = {start}
    queue = [start]
    while queue:
        node = queue.pop(0)
        for neighbor in sorted(adjacency[node] - visited):
            visited.add(neighbor)
            queue.append(neighbor)
    return visited


def invariant_violations(topology_value: Any) -> list[dict[str, Any]]:
    topology = parse_topology(topology_value)
    req = topology["requirements"]
    backbone = _reachable(topology, req["core"], "backbone")
    management = _reachable(topology, req["operator"], "management")
    disconnected_dc = sorted(set(req["datacenters"]) - backbone)
    disconnected_mgmt = sorted(set(req["management_targets"]) - management)
    active_dns = sorted(set(req["dns_nodes"]) & backbone)
    violations: list[dict[str, Any]] = []
    if len(disconnected_dc) > req["max_disconnected_datacenters"]:
        violations.append({
            "code": "BACKBONE_REACHABILITY",
            "disconnected": disconnected_dc,
            "allowed": req["max_disconnected_datacenters"],
        })
    if len(active_dns) < req["min_dns_advertisers"]:
        violations.append({
            "code": "DNS_ADVERTISEMENT_CAPACITY",
            "active": active_dns,
            "required": req["min_dns_advertisers"],
        })
    if len(disconnected_mgmt) > req["max_disconnected_management"]:
        violations.append({
            "code": "RECOVERY_PATH",
            "disconnected": disconnected_mgmt,
            "allowed": req["max_disconnected_management"],
        })
    return violations


def _set_link(topology: dict[str, Any], link_id: str, up: bool) -> bool:
    for link in topology["links"]:
        if link["id"] == link_id:
            previous = link["up"]
            link["up"] = up
            return previous
    raise AnalysisFailure(f"plan references unknown link: {link_id}")


def _failure_domains(topology: dict[str, Any]) -> list[str]:
    return sorted({link["failure_domain"] for link in topology["links"] if link["up"]})


def _disable_domain(topology: dict[str, Any], domain: str) -> None:
    for link in topology["links"]:
        if link["failure_domain"] == domain:
            link["up"] = False


def analyze(topology_value: Any, plan_value: Any) -> dict[str, Any]:
    topology = parse_topology(topology_value)
    plan = parse_plan(plan_value)
    if plan["expected_topology_version"] != topology["version"]:
        return {
            "safe": False,
            "plan_hash": digest(plan),
            "snapshot_version": topology["version"],
            "violations": [{
                "code": "STALE_TOPOLOGY",
                "expected": plan["expected_topology_version"],
                "actual": topology["version"],
            }],
            "steps": [],
        }

    violations: list[dict[str, Any]] = []
    baseline = invariant_violations(topology)
    if baseline:
        violations.append({"code": "UNHEALTHY_BASELINE", "details": baseline})

    state = deepcopy(topology)
    step_results: list[dict[str, Any]] = []
    seen_targets: set[tuple[str, bool]] = set()
    for index, step in enumerate(plan["steps"]):
        link_key = (step["link_id"], step["up"])
        if link_key in seen_targets:
            violations.append({"code": "REPEATED_OPERATION", "step": index, "link_id": step["link_id"]})
        seen_targets.add(link_key)
        previous = _set_link(state, step["link_id"], step["up"])
        direct = invariant_violations(state)
        if previous == step["up"]:
            direct = [{"code": "NO_OP", "link_id": step["link_id"]}, *direct]

        contingency_failures: list[dict[str, Any]] = []
        if state["requirements"]["require_single_failure_tolerance"]:
            for domain in _failure_domains(state):
                candidate = deepcopy(state)
                _disable_domain(candidate, domain)
                broken = invariant_violations(candidate)
                if broken:
                    contingency_failures.append({"failure_domain": domain, "violations": broken})
        result = {
            "index": index,
            "link_id": step["link_id"],
            "requested_up": step["up"],
            "previous_up": previous,
            "violations": direct,
            "contingency_failures": contingency_failures[:20],
        }
        step_results.append(result)
        if direct:
            violations.append({"code": "STEP_INVARIANT", "step": index, "details": direct})
        if contingency_failures:
            violations.append({
                "code": "SINGLE_FAILURE_TOLERANCE",
                "step": index,
                "failed_domains": [item["failure_domain"] for item in contingency_failures[:20]],
            })

    return {
        "safe": not violations,
        "plan_hash": digest(plan),
        "snapshot_version": topology["version"],
        "violations": violations,
        "steps": step_results,
    }
