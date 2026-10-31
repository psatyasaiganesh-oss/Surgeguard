from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any

from .errors import ValidationError


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
EVENT_KINDS = {"USER", "TOOL_RESULT", "CHILD_REPORT", "TIMER"}
COMMAND_TYPES = {"TOOL", "SEND", "CLOSE"}
MAX_JSON_BYTES = 262_144
MAX_MESSAGE_BYTES = 65_536


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
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{name} must be at most {maximum}")
    return value


def require_finite_number(value: Any, name: str, *, minimum: float = 0) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or float(value) < minimum
    ):
        raise ValidationError(f"{name} must be a finite number >= {minimum}")
    return float(value)


def require_payload(value: Any, name: str = "payload") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    canonical_json(value)
    return value


def require_message(value: Any, name: str = "message") -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{name} must be a non-empty string")
    if len(value.encode()) > MAX_MESSAGE_BYTES:
        raise ValidationError(f"{name} exceeds {MAX_MESSAGE_BYTES} UTF-8 bytes")
    return value


def require_event_kind(value: Any) -> str:
    if value not in EVENT_KINDS:
        raise ValidationError(f"kind must be one of {', '.join(sorted(EVENT_KINDS))}")
    return value


@dataclass(frozen=True)
class SliceDecision:
    assistant_message: str
    command: dict[str, Any] | None = None

    @classmethod
    def from_value(cls, value: Any) -> "SliceDecision":
        body = require_closed_object(value, "decision", {"assistant_message"}, {"command"})
        message = require_message(body["assistant_message"], "assistant_message")
        raw_command = body.get("command")
        if raw_command is None:
            return cls(message)
        command = require_closed_object(raw_command, "command", {"type"}, {
            "tool_name",
            "arguments",
            "target_thread_id",
            "payload",
        })
        command_type = command["type"]
        if command_type not in COMMAND_TYPES:
            raise ValidationError(
                f"command.type must be one of {', '.join(sorted(COMMAND_TYPES))}"
            )
        if command_type == "TOOL":
            if set(command) != {"type", "tool_name", "arguments"}:
                raise ValidationError("TOOL requires only tool_name and arguments")
            normalized = {
                "type": "TOOL",
                "tool_name": require_identifier(command["tool_name"], "tool_name"),
                "arguments": require_payload(command["arguments"], "arguments"),
            }
        elif command_type == "SEND":
            if set(command) != {"type", "target_thread_id", "payload"}:
                raise ValidationError("SEND requires only target_thread_id and payload")
            normalized = {
                "type": "SEND",
                "target_thread_id": require_identifier(
                    command["target_thread_id"], "target_thread_id"
                ),
                "payload": require_payload(command["payload"]),
            }
        else:
            if set(command) != {"type"}:
                raise ValidationError("CLOSE does not accept additional fields")
            normalized = {"type": "CLOSE"}
        return cls(message, normalized)

    def as_dict(self) -> dict[str, Any]:
        return {
            "assistant_message": self.assistant_message,
            "command": self.command,
        }
