from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any

from .errors import ValidationError


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
POLICIES = {"BEST_FIT", "LIFETIME"}
MAX_JSON_BYTES = 262_144
MAX_RESOURCE = 10_000_000
MAX_LIFETIME_S = 3_600_000
LIFETIME_UPPER_S = (3_600, 36_000, 360_000, 3_600_000)


def canonical_json(value: Any) -> str:
    try:
        rendered = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError("value is not canonical JSON") from exc
    if len(rendered.encode()) > MAX_JSON_BYTES:
        raise ValidationError(f"canonical JSON exceeds {MAX_JSON_BYTES} bytes")
    return rendered


def parse_json(raw: str, name: str = "JSON") -> Any:
    def reject_constant(value: str) -> None:
        raise ValidationError(f"{name} contains non-finite number {value}")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"{name} contains duplicate field {key}")
            result[key] = value
        return result

    try:
        return json.loads(
            raw,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicates,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValidationError(f"invalid {name}") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def require_closed_object(
    value: Any,
    name: str,
    required: set[str],
    optional: set[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    optional = optional or set()
    missing = required - set(value)
    unknown = set(value) - required - optional
    if missing:
        raise ValidationError(f"{name} is missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ValidationError(f"{name} has unknown fields: {', '.join(sorted(unknown))}")
    return value


def require_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(
            f"{name} must be 1-128 characters using letters, numbers, '.', '_', ':', or '-'"
        )
    return value


def require_int(
    value: Any,
    name: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if value < minimum:
        raise ValidationError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{name} must be at most {maximum}")
    return value


def require_policy(value: Any) -> str:
    if value not in POLICIES:
        raise ValidationError(f"policy must be one of {', '.join(sorted(POLICIES))}")
    return value


def lifetime_class(seconds: int) -> int:
    seconds = require_int(seconds, "predicted_lifetime_s", minimum=1, maximum=MAX_LIFETIME_S)
    for index, upper in enumerate(LIFETIME_UPPER_S):
        if seconds <= upper:
            return index
    return len(LIFETIME_UPPER_S) - 1


@dataclass(frozen=True)
class VMRequest:
    vm_id: str
    cpu_millis: int
    memory_mib: int
    predicted_lifetime_s: int

    @classmethod
    def from_value(cls, value: Any) -> "VMRequest":
        body = require_closed_object(
            value,
            "vm",
            {"vm_id", "cpu_millis", "memory_mib", "predicted_lifetime_s"},
        )
        return cls(
            vm_id=require_identifier(body["vm_id"], "vm_id"),
            cpu_millis=require_int(
                body["cpu_millis"], "cpu_millis", minimum=1, maximum=MAX_RESOURCE
            ),
            memory_mib=require_int(
                body["memory_mib"], "memory_mib", minimum=1, maximum=MAX_RESOURCE
            ),
            predicted_lifetime_s=require_int(
                body["predicted_lifetime_s"],
                "predicted_lifetime_s",
                minimum=1,
                maximum=MAX_LIFETIME_S,
            ),
        )

    @property
    def lifetime_class(self) -> int:
        return lifetime_class(self.predicted_lifetime_s)

    def as_dict(self) -> dict[str, int | str]:
        return {
            "vm_id": self.vm_id,
            "cpu_millis": self.cpu_millis,
            "memory_mib": self.memory_mib,
            "predicted_lifetime_s": self.predicted_lifetime_s,
        }


def finite_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    result = numerator / denominator
    if not math.isfinite(result):
        raise ValidationError("ratio is not finite")
    return result
