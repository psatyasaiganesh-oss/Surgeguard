"""Strict schemas and canonical hashing for RouteProof."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$")
PLAN_ACTIONS = {"set_link"}
PLANES = {"backbone", "management"}
NODE_KINDS = {"core", "datacenter", "dns", "noc", "transit"}
MAX_JSON_BYTES = 65_536
MAX_NODES = 100
MAX_LINKS = 500
MAX_STEPS = 50


class ValidationError(ValueError):
    """Raised when untrusted configuration does not match the closed schema."""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def strict_json(raw: bytes | str) -> Any:
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if len(raw) > MAX_JSON_BYTES:
        raise ValidationError("JSON body exceeds 65536 bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError("JSON must be UTF-8") from exc

    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in pairs:
            if key in out:
                raise ValidationError(f"duplicate JSON key: {key}")
            out[key] = value
        return out

    try:
        return json.loads(text, object_pairs_hook=unique_pairs)
    except ValidationError:
        raise
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ValidationError("invalid JSON") from exc


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    return value


def _closed(obj: dict[str, Any], required: set[str], name: str) -> None:
    missing = required - obj.keys()
    extra = obj.keys() - required
    if missing:
        raise ValidationError(f"{name} missing fields: {sorted(missing)}")
    if extra:
        raise ValidationError(f"{name} has unknown fields: {sorted(extra)}")


def _id(value: Any, name: str) -> str:
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValidationError(f"{name} must match {ID_RE.pattern}")
    return value


def _int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def _bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{name} must be boolean")
    return value


def _id_list(value: Any, name: str, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise ValidationError(f"{name} must be a non-empty list")
    result = [_id(item, f"{name} item") for item in value]
    if len(result) != len(set(result)):
        raise ValidationError(f"{name} contains duplicates")
    return result


def parse_topology(value: Any) -> dict[str, Any]:
    root = _object(value, "topology")
    _closed(root, {"version", "nodes", "links", "requirements"}, "topology")
    version = _int(root["version"], "topology.version", 1, 2_147_483_647)

    nodes_raw = root["nodes"]
    if not isinstance(nodes_raw, list) or not 3 <= len(nodes_raw) <= MAX_NODES:
        raise ValidationError(f"topology.nodes must contain 3..{MAX_NODES} nodes")
    nodes: list[dict[str, str]] = []
    node_ids: set[str] = set()
    for index, raw in enumerate(nodes_raw):
        node = _object(raw, f"node[{index}]")
        _closed(node, {"id", "kind"}, f"node[{index}]")
        node_id = _id(node["id"], f"node[{index}].id")
        if node_id in node_ids:
            raise ValidationError(f"duplicate node id: {node_id}")
        if node["kind"] not in NODE_KINDS:
            raise ValidationError(f"node[{index}].kind is unsupported")
        node_ids.add(node_id)
        nodes.append({"id": node_id, "kind": node["kind"]})

    links_raw = root["links"]
    if not isinstance(links_raw, list) or not 1 <= len(links_raw) <= MAX_LINKS:
        raise ValidationError(f"topology.links must contain 1..{MAX_LINKS} links")
    links: list[dict[str, Any]] = []
    link_ids: set[str] = set()
    endpoint_keys: set[tuple[str, str, str, str]] = set()
    for index, raw in enumerate(links_raw):
        link = _object(raw, f"link[{index}]")
        _closed(link, {"id", "a", "b", "plane", "up", "failure_domain"}, f"link[{index}]")
        link_id = _id(link["id"], f"link[{index}].id")
        a = _id(link["a"], f"link[{index}].a")
        b = _id(link["b"], f"link[{index}].b")
        domain = _id(link["failure_domain"], f"link[{index}].failure_domain")
        if link_id in link_ids:
            raise ValidationError(f"duplicate link id: {link_id}")
        if a not in node_ids or b not in node_ids or a == b:
            raise ValidationError(f"link {link_id} has invalid endpoints")
        if link["plane"] not in PLANES:
            raise ValidationError(f"link {link_id} has unsupported plane")
        key = (min(a, b), max(a, b), link["plane"], domain)
        if key in endpoint_keys:
            raise ValidationError(f"duplicate endpoint/domain tuple for link {link_id}")
        endpoint_keys.add(key)
        link_ids.add(link_id)
        links.append({
            "id": link_id,
            "a": a,
            "b": b,
            "plane": link["plane"],
            "up": _bool(link["up"], f"link[{index}].up"),
            "failure_domain": domain,
        })

    req = _object(root["requirements"], "requirements")
    _closed(
        req,
        {
            "core",
            "datacenters",
            "dns_nodes",
            "operator",
            "management_targets",
            "min_dns_advertisers",
            "max_disconnected_datacenters",
            "max_disconnected_management",
            "require_single_failure_tolerance",
        },
        "requirements",
    )
    core = _id(req["core"], "requirements.core")
    operator = _id(req["operator"], "requirements.operator")
    datacenters = _id_list(req["datacenters"], "requirements.datacenters")
    dns_nodes = _id_list(req["dns_nodes"], "requirements.dns_nodes")
    targets = _id_list(req["management_targets"], "requirements.management_targets")
    referenced = {core, operator, *datacenters, *dns_nodes, *targets}
    missing_nodes = referenced - node_ids
    if missing_nodes:
        raise ValidationError(f"requirements reference unknown nodes: {sorted(missing_nodes)}")
    kinds = {item["id"]: item["kind"] for item in nodes}
    if kinds[core] != "core" or kinds[operator] != "noc":
        raise ValidationError("core/operator node kinds do not match requirements")
    if any(kinds[item] != "datacenter" for item in datacenters):
        raise ValidationError("all datacenters must have datacenter kind")
    if any(kinds[item] != "dns" for item in dns_nodes):
        raise ValidationError("all dns_nodes must have dns kind")
    min_dns = _int(req["min_dns_advertisers"], "requirements.min_dns_advertisers", 1, len(dns_nodes))
    max_dc = _int(
        req["max_disconnected_datacenters"],
        "requirements.max_disconnected_datacenters",
        0,
        len(datacenters),
    )
    max_mgmt = _int(
        req["max_disconnected_management"],
        "requirements.max_disconnected_management",
        0,
        len(targets),
    )
    normalized_req = {
        "core": core,
        "datacenters": datacenters,
        "dns_nodes": dns_nodes,
        "operator": operator,
        "management_targets": targets,
        "min_dns_advertisers": min_dns,
        "max_disconnected_datacenters": max_dc,
        "max_disconnected_management": max_mgmt,
        "require_single_failure_tolerance": _bool(
            req["require_single_failure_tolerance"],
            "requirements.require_single_failure_tolerance",
        ),
    }
    return {"version": version, "nodes": nodes, "links": links, "requirements": normalized_req}


def parse_plan(value: Any) -> dict[str, Any]:
    root = _object(value, "plan")
    _closed(root, {"plan_id", "reason", "expected_topology_version", "steps"}, "plan")
    plan_id = _id(root["plan_id"], "plan.plan_id")
    reason = root["reason"]
    if not isinstance(reason, str) or not 8 <= len(reason.strip()) <= 500:
        raise ValidationError("plan.reason must contain 8..500 characters")
    expected = _int(root["expected_topology_version"], "plan.expected_topology_version", 1, 2_147_483_647)
    raw_steps = root["steps"]
    if not isinstance(raw_steps, list) or not 1 <= len(raw_steps) <= MAX_STEPS:
        raise ValidationError(f"plan.steps must contain 1..{MAX_STEPS} operations")
    steps: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_steps):
        step = _object(raw, f"step[{index}]")
        _closed(step, {"action", "link_id", "up"}, f"step[{index}]")
        if step["action"] not in PLAN_ACTIONS:
            raise ValidationError(f"step[{index}].action is unsupported")
        steps.append({
            "action": step["action"],
            "link_id": _id(step["link_id"], f"step[{index}].link_id"),
            "up": _bool(step["up"], f"step[{index}].up"),
        })
    return {
        "plan_id": plan_id,
        "reason": reason.strip(),
        "expected_topology_version": expected,
        "steps": steps,
    }
