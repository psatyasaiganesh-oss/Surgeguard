"""Closed schemas and canonical values for SpecuFlow."""

from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any

ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
MAX_REQUEST_BYTES = 1_048_576
MAX_SHARDS = 1_000
MAX_PAYLOAD_BYTES = 16_384
MAX_DEPTH = 10


class ValidationError(ValueError):
    """Untrusted data does not satisfy SpecuFlow's closed contract."""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    raw = value if isinstance(value, bytes) else canonical(value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw: bytes | str) -> Any:
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if len(raw) > MAX_REQUEST_BYTES:
        raise ValidationError("JSON exceeds 1048576 bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError("JSON must be UTF-8") from exc

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(text, object_pairs_hook=unique)
    except ValidationError:
        raise
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ValidationError("invalid JSON") from exc


def identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValidationError(f"{name} must match {ID_RE.pattern}")
    return value


def integer(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def closed(obj: Any, fields: set[str], name: str) -> dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValidationError(f"{name} must be an object")
    missing = fields - obj.keys()
    extra = obj.keys() - fields
    if missing:
        raise ValidationError(f"{name} missing fields: {sorted(missing)}")
    if extra:
        raise ValidationError(f"{name} has unknown fields: {sorted(extra)}")
    return obj


def _validate_json_value(value: Any, depth: int = 0) -> None:
    if depth > MAX_DEPTH:
        raise ValidationError("payload nesting is too deep")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("payload numbers must be finite")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item, depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > 128:
                raise ValidationError("payload keys must be strings of at most 128 characters")
            _validate_json_value(item, depth + 1)
        return
    raise ValidationError("payload contains an unsupported JSON value")


def parse_job(value: Any) -> dict[str, Any]:
    root = closed(value, {"job_id", "shards", "policy"}, "job")
    job_id = identifier(root["job_id"], "job.job_id")
    shards_raw = root["shards"]
    if not isinstance(shards_raw, list) or not 1 <= len(shards_raw) <= MAX_SHARDS:
        raise ValidationError(f"job.shards must contain 1..{MAX_SHARDS} items")
    shards: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(shards_raw):
        shard = closed(raw, {"shard_id", "payload"}, f"shard[{index}]")
        shard_id = identifier(shard["shard_id"], f"shard[{index}].shard_id")
        if shard_id in seen:
            raise ValidationError(f"duplicate shard id: {shard_id}")
        seen.add(shard_id)
        _validate_json_value(shard["payload"])
        if len(canonical(shard["payload"]).encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValidationError(f"payload for {shard_id} exceeds {MAX_PAYLOAD_BYTES} bytes")
        shards.append({"shard_id": shard_id, "payload": shard["payload"]})

    policy = closed(
        root["policy"],
        {"lease_ms", "speculate_after_ms", "max_attempts", "max_speculative_attempts"},
        "job.policy",
    )
    lease_ms = integer(policy["lease_ms"], "policy.lease_ms", 50, 60_000)
    speculate_after = integer(
        policy["speculate_after_ms"], "policy.speculate_after_ms", 10, 59_999
    )
    if speculate_after >= lease_ms:
        raise ValidationError("speculate_after_ms must be less than lease_ms")
    normalized_policy = {
        "lease_ms": lease_ms,
        "speculate_after_ms": speculate_after,
        "max_attempts": integer(policy["max_attempts"], "policy.max_attempts", 1, 5),
        "max_speculative_attempts": integer(
            policy["max_speculative_attempts"],
            "policy.max_speculative_attempts",
            0,
            2,
        ),
    }
    return {"job_id": job_id, "shards": shards, "policy": normalized_policy}


def parse_output(raw: bytes, expected_shard: str) -> dict[str, Any]:
    value = strict_json(raw)
    obj = closed(value, {"shard_id", "value"}, "output")
    if identifier(obj["shard_id"], "output.shard_id") != expected_shard:
        raise ValidationError("output shard_id does not match the claimed task")
    _validate_json_value(obj["value"])
    return obj


def parse_counters(value: Any) -> dict[str, int]:
    if not isinstance(value, dict) or len(value) > 50:
        raise ValidationError("counters must be an object with at most 50 entries")
    result: dict[str, int] = {}
    for key, count in value.items():
        name = identifier(key, "counter name")
        result[name] = integer(count, f"counter {name}", 0, 1_000_000_000_000)
    return result
