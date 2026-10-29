from __future__ import annotations

import hashlib
import json
import math
import posixpath
import re
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import ValidationError


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
TEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_./:\[\]-]{0,255}$")
HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
MAX_TESTS = 10_000
MAX_FILES = 20_000
MAX_DEPENDENCIES_PER_TEST = 2_000
MAX_PATH_BYTES = 512


def require_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(
            f"{name} must be 1-128 characters using letters, numbers, '.', '_', ':', or '-'"
        )
    return value


def require_test_id(value: Any, name: str = "test_id") -> str:
    if not isinstance(value, str) or not TEST_ID.fullmatch(value):
        raise ValidationError(f"{name} has an invalid test identifier")
    if ".." in value.split("/"):
        raise ValidationError(f"{name} cannot contain parent traversal")
    return value


def require_int(
    value: Any, name: str, *, minimum: int | None = None, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{name} must be at most {maximum}")
    return value


def require_timestamp(value: Any, name: str) -> int:
    return require_int(value, name, minimum=0)


def require_digest(value: Any, name: str = "digest") -> str:
    if not isinstance(value, str) or not HEX_DIGEST.fullmatch(value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 hex digest")
    return value


def require_closed_object(
    value: Any, name: str, required: set[str], optional: set[str] | None = None
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    optional = optional or set()
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        raise ValidationError(f"{name} is missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ValidationError(f"{name} has unknown fields: {', '.join(sorted(unknown))}")
    return value


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
    if len(rendered.encode()) > 4_194_304:
        raise ValidationError("canonical JSON exceeds 4 MiB")
    return rendered


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


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


def normalize_path(value: Any, name: str = "path") -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValidationError(f"{name} must be a non-empty string")
    if "\\" in value:
        raise ValidationError(f"{name} must use '/' separators")
    if value.startswith("/") or value.endswith("/"):
        raise ValidationError(f"{name} must be a relative file path")
    if len(value.encode()) > MAX_PATH_BYTES:
        raise ValidationError(f"{name} exceeds {MAX_PATH_BYTES} bytes")
    normalized = posixpath.normpath(value)
    if normalized in {".", ".."} or normalized.startswith("../") or normalized != value:
        raise ValidationError(f"{name} must be normalized and cannot traverse parents")
    if any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise ValidationError(f"{name} is invalid")
    return normalized


def normalize_inventory(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise ValidationError("inventory must be a non-empty path-to-digest object")
    if len(value) > MAX_FILES:
        raise ValidationError(f"inventory exceeds {MAX_FILES} files")
    result: dict[str, str] = {}
    for raw_path, raw_digest in value.items():
        path = normalize_path(raw_path, "inventory path")
        if path in result:
            raise ValidationError("inventory contains duplicate normalized paths")
        result[path] = require_digest(raw_digest, f"digest for {path}")
    return dict(sorted(result.items()))


@dataclass(frozen=True)
class TestSpec:
    test_id: str
    estimated_ms: float
    mandatory: bool = False
    last_status: str = "PASS"

    @classmethod
    def from_value(cls, value: Any) -> "TestSpec":
        body = require_closed_object(
            value,
            "test",
            {"test_id", "estimated_ms", "mandatory", "last_status"},
        )
        estimated = body["estimated_ms"]
        if (
            isinstance(estimated, bool)
            or not isinstance(estimated, (int, float))
            or not math.isfinite(estimated)
            or not 0.01 <= float(estimated) <= 3_600_000
        ):
            raise ValidationError("estimated_ms must be finite and between 0.01 and 3,600,000")
        if not isinstance(body["mandatory"], bool):
            raise ValidationError("mandatory must be boolean")
        if body["last_status"] not in {"PASS", "FAIL", "UNKNOWN"}:
            raise ValidationError("last_status must be PASS, FAIL, or UNKNOWN")
        return cls(
            require_test_id(body["test_id"]),
            float(estimated),
            body["mandatory"],
            body["last_status"],
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "test_id": self.test_id,
            "estimated_ms": self.estimated_ms,
            "mandatory": self.mandatory,
            "last_status": self.last_status,
        }


@dataclass(frozen=True)
class BaselineSnapshot:
    inventory: dict[str, str]
    tests: tuple[TestSpec, ...]
    observations: dict[str, tuple[str, ...]]
    root_files: tuple[str, ...]
    completed_commit: str

    @classmethod
    def from_value(cls, value: Any) -> "BaselineSnapshot":
        body = require_closed_object(
            value,
            "baseline",
            {"inventory", "tests", "observations", "root_files", "completed_commit"},
        )
        inventory = normalize_inventory(body["inventory"])
        raw_tests = body["tests"]
        if not isinstance(raw_tests, list) or not raw_tests:
            raise ValidationError("tests must be a non-empty array")
        if len(raw_tests) > MAX_TESTS:
            raise ValidationError(f"tests exceeds {MAX_TESTS}")
        tests = tuple(TestSpec.from_value(item) for item in raw_tests)
        test_ids = [item.test_id for item in tests]
        if len(test_ids) != len(set(test_ids)):
            raise ValidationError("test_id values must be unique")
        raw_observations = body["observations"]
        if not isinstance(raw_observations, dict):
            raise ValidationError("observations must be an object")
        unknown_tests = set(raw_observations) - set(test_ids)
        if unknown_tests:
            raise ValidationError(
                f"observations contain unknown tests: {', '.join(sorted(unknown_tests))}"
            )
        observations: dict[str, tuple[str, ...]] = {}
        for test_id, raw_paths in raw_observations.items():
            require_test_id(test_id)
            if not isinstance(raw_paths, list) or len(raw_paths) > MAX_DEPENDENCIES_PER_TEST:
                raise ValidationError(
                    f"dependencies for {test_id} must be an array of at most {MAX_DEPENDENCIES_PER_TEST}"
                )
            paths = tuple(sorted(normalize_path(path, "dependency path") for path in raw_paths))
            if len(paths) != len(set(paths)):
                raise ValidationError(f"dependencies for {test_id} must be unique")
            missing = sorted(set(paths) - set(inventory))
            if missing:
                raise ValidationError(
                    f"dependencies for {test_id} are absent from inventory: {', '.join(missing[:5])}"
                )
            observations[test_id] = paths
        raw_root = body["root_files"]
        if not isinstance(raw_root, list):
            raise ValidationError("root_files must be an array")
        root_files = tuple(sorted(normalize_path(path, "root file") for path in raw_root))
        if len(root_files) != len(set(root_files)):
            raise ValidationError("root_files must be unique")
        missing_root = sorted(set(root_files) - set(inventory))
        if missing_root:
            raise ValidationError("root_files must exist in inventory")
        commit = require_identifier(body["completed_commit"], "completed_commit")
        return cls(inventory, tuple(sorted(tests, key=lambda item: item.test_id)), observations, root_files, commit)

    def as_dict(self) -> dict[str, Any]:
        return {
            "inventory": self.inventory,
            "tests": [item.as_dict() for item in self.tests],
            "observations": {
                test_id: list(paths) for test_id, paths in sorted(self.observations.items())
            },
            "root_files": list(self.root_files),
            "completed_commit": self.completed_commit,
        }


def normalize_test_ids(values: Any, known: Iterable[str], name: str) -> tuple[str, ...]:
    if not isinstance(values, list):
        raise ValidationError(f"{name} must be an array")
    known_set = set(known)
    result = tuple(sorted(require_test_id(value, name) for value in values))
    if len(result) != len(set(result)):
        raise ValidationError(f"{name} must be unique")
    unknown = sorted(set(result) - known_set)
    if unknown:
        raise ValidationError(f"{name} contains unknown tests: {', '.join(unknown[:5])}")
    return result
