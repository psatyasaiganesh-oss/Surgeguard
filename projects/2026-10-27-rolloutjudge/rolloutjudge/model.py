"""Validated policy models and deterministic serialization."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import ValidationError


ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_JSON_BYTES = 524_288
MAX_SERIES_VALUES = 5_000
MAX_TIMESTAMP_NS = 9_223_372_036_854_775_807
DIRECTIONS = {"lower", "higher", "either"}
MISSING_STRATEGIES = {"drop", "zero"}


def validate_id(value: str, field: str) -> str:
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValidationError(
            f"{field} must match {ID_RE.pattern} and be at most 96 characters"
        )
    return value


def validate_integer(value: int, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} must be an integer")
    if not minimum <= value <= maximum:
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def validate_number(
    value: int | float, field: str, minimum: float, maximum: float
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{field} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return result


def validate_timestamp(value: int, field: str = "timestamp") -> int:
    return validate_integer(value, field, 0, MAX_TIMESTAMP_NS)


def canonical_json(value: Any, *, max_bytes: int = MAX_JSON_BYTES) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError("value must be finite JSON") from exc
    if len(encoded.encode("utf-8")) > max_bytes:
        raise ValidationError(f"JSON exceeds {max_bytes} UTF-8 bytes")
    return encoded


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def validate_series(values: Any, field: str) -> tuple[float | None, ...]:
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_SERIES_VALUES:
        raise ValidationError(
            f"{field} must be an array containing 1 to {MAX_SERIES_VALUES} values"
        )
    result: list[float | None] = []
    for index, value in enumerate(values):
        if value is None:
            result.append(None)
        else:
            result.append(validate_number(value, f"{field}[{index}]", -1e15, 1e15))
    return tuple(result)


@dataclass(frozen=True)
class MetricRule:
    name: str
    direction: str
    group: str = "service"
    weight: int = 1
    critical: bool = False
    alpha: float = 0.05
    min_samples: int = 20
    max_missing_fraction: float = 0.1
    max_relative_degradation: float = 0.1
    max_absolute_degradation: float = 0.0
    relative_floor: float = 1e-9
    missing_strategy: str = "drop"

    def validated(self) -> "MetricRule":
        validate_id(self.name, "metric name")
        validate_id(self.group, "metric group")
        if self.direction not in DIRECTIONS:
            raise ValidationError(f"direction must be one of {sorted(DIRECTIONS)}")
        validate_integer(self.weight, "weight", 1, 100)
        if not isinstance(self.critical, bool):
            raise ValidationError("critical must be boolean")
        validate_number(self.alpha, "alpha", 0.0001, 0.25)
        validate_integer(self.min_samples, "min_samples", 5, MAX_SERIES_VALUES)
        validate_number(
            self.max_missing_fraction, "max_missing_fraction", 0.0, 0.95
        )
        validate_number(
            self.max_relative_degradation, "max_relative_degradation", 0.0, 100.0
        )
        validate_number(
            self.max_absolute_degradation, "max_absolute_degradation", 0.0, 1e15
        )
        validate_number(self.relative_floor, "relative_floor", 1e-12, 1e15)
        if self.missing_strategy not in MISSING_STRATEGIES:
            raise ValidationError(
                f"missing_strategy must be one of {sorted(MISSING_STRATEGIES)}"
            )
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "alpha": self.alpha,
            "critical": self.critical,
            "direction": self.direction,
            "group": self.group,
            "max_absolute_degradation": self.max_absolute_degradation,
            "max_missing_fraction": self.max_missing_fraction,
            "max_relative_degradation": self.max_relative_degradation,
            "min_samples": self.min_samples,
            "missing_strategy": self.missing_strategy,
            "name": self.name,
            "relative_floor": self.relative_floor,
            "weight": self.weight,
        }

    @classmethod
    def from_dict(cls, value: Any) -> "MetricRule":
        if not isinstance(value, dict):
            raise ValidationError("each metric rule must be an object")
        allowed = {
            "alpha",
            "critical",
            "direction",
            "group",
            "max_absolute_degradation",
            "max_missing_fraction",
            "max_relative_degradation",
            "min_samples",
            "missing_strategy",
            "name",
            "relative_floor",
            "weight",
        }
        unknown = value.keys() - allowed
        if unknown:
            raise ValidationError(f"unknown metric fields: {', '.join(sorted(unknown))}")
        if "name" not in value or "direction" not in value:
            raise ValidationError("metric rules require name and direction")
        return cls(**value).validated()


@dataclass(frozen=True)
class Policy:
    metrics: tuple[MetricRule, ...]
    pass_score: float = 90.0
    manual_score: float = 70.0
    false_discovery_rate: float = 0.05

    def validated(self) -> "Policy":
        if not 1 <= len(self.metrics) <= 50:
            raise ValidationError("policy must define between 1 and 50 metrics")
        validated = tuple(metric.validated() for metric in self.metrics)
        names = [metric.name for metric in validated]
        if len(names) != len(set(names)):
            raise ValidationError("metric names must be unique")
        pass_score = validate_number(self.pass_score, "pass_score", 0.0, 100.0)
        manual_score = validate_number(self.manual_score, "manual_score", 0.0, 100.0)
        validate_number(
            self.false_discovery_rate, "false_discovery_rate", 0.0001, 0.25
        )
        if manual_score >= pass_score:
            raise ValidationError("manual_score must be lower than pass_score")
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "false_discovery_rate": self.false_discovery_rate,
            "manual_score": self.manual_score,
            "metrics": [metric.as_dict() for metric in self.metrics],
            "pass_score": self.pass_score,
        }

    @classmethod
    def from_dict(cls, value: Any) -> "Policy":
        if not isinstance(value, dict):
            raise ValidationError("policy must be an object")
        allowed = {"false_discovery_rate", "manual_score", "metrics", "pass_score"}
        unknown = value.keys() - allowed
        if unknown:
            raise ValidationError(f"unknown policy fields: {', '.join(sorted(unknown))}")
        if "metrics" not in value or not isinstance(value["metrics"], list):
            raise ValidationError("policy metrics must be an array")
        return cls(
            metrics=tuple(MetricRule.from_dict(metric) for metric in value["metrics"]),
            pass_score=value.get("pass_score", 90.0),
            manual_score=value.get("manual_score", 70.0),
            false_discovery_rate=value.get("false_discovery_rate", 0.05),
        ).validated()


def validate_trials(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValidationError("expected_trials must be an array")
    trials = tuple(validate_id(value, "trial") for value in values)
    if not 1 <= len(trials) <= 20:
        raise ValidationError("expected_trials must contain between 1 and 20 trials")
    if len(trials) != len(set(trials)):
        raise ValidationError("expected trial identifiers must be unique")
    return tuple(sorted(trials))
