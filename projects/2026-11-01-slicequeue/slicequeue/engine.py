from __future__ import annotations

import math
from typing import Any

from .errors import ValidationError
from .model import MAX_MESSAGE_BYTES, canonical_json, require_identifier, require_payload


MAX_SUM_ITEMS = 10_000


def _error(message: str) -> dict[str, Any]:
    return {"assistant_message": f"Rejected event: {message}", "command": None}


def deterministic_handler(
    history: list[dict[str, Any]], event: dict[str, Any]
) -> dict[str, Any]:
    """A credential-free policy used by the demo and local HTTP service.

    SliceQueue is a runtime, not an LLM.  This deliberately small policy makes the
    scheduling, persistence, and tool lifecycle reproducible without a model API.
    """

    del history  # A real model adapter would construct its prompt from this history.
    kind = event.get("kind")
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return _error("payload must be an object")

    if kind == "USER":
        action = payload.get("action")
        if action == "sum":
            if set(payload) != {"action", "numbers"}:
                return _error("sum accepts only action and numbers")
            return {
                "assistant_message": "The sum is running asynchronously.",
                "command": {
                    "type": "TOOL",
                    "tool_name": "math.sum",
                    "arguments": {"numbers": payload["numbers"]},
                },
            }
        if action == "word_count":
            if set(payload) != {"action", "text"}:
                return _error("word_count accepts only action and text")
            return {
                "assistant_message": "The word count is running asynchronously.",
                "command": {
                    "type": "TOOL",
                    "tool_name": "text.word_count",
                    "arguments": {"text": payload["text"]},
                },
            }
        if action == "send":
            if set(payload) != {"action", "target_thread_id", "payload"}:
                return _error("send requires target_thread_id and payload")
            try:
                target = require_identifier(payload["target_thread_id"], "target_thread_id")
                message = require_payload(payload["payload"], "payload.payload")
            except ValidationError as exc:
                return _error(str(exc))
            return {
                "assistant_message": f"Queued a durable message for {target}.",
                "command": {
                    "type": "SEND",
                    "target_thread_id": target,
                    "payload": message,
                },
            }
        if action == "close":
            if set(payload) != {"action"}:
                return _error("close accepts only action")
            return {
                "assistant_message": "Closing this thread after this turn.",
                "command": {"type": "CLOSE"},
            }
        if action == "ping":
            if set(payload) != {"action"}:
                return _error("ping accepts only action")
            return {"assistant_message": "pong", "command": None}
        return _error("unknown USER action")

    if kind == "TOOL_RESULT":
        expected = {"call_id", "tool_name", "result"}
        if set(payload) != expected:
            return _error("invalid tool result envelope")
        result = canonical_json(payload["result"])
        if len(result.encode()) > MAX_MESSAGE_BYTES - 64:
            result = '"result omitted because it is too large"'
        return {
            "assistant_message": f"Tool {payload['tool_name']} completed: {result}",
            "command": None,
        }

    if kind == "CHILD_REPORT":
        if set(payload) != {"source_thread_id", "payload"}:
            return _error("invalid inter-thread message envelope")
        return {
            "assistant_message": (
                f"Received a durable message from {payload['source_thread_id']}: "
                f"{canonical_json(payload['payload'])}"
            ),
            "command": None,
        }

    if kind == "TIMER":
        return {
            "assistant_message": f"Timer fired: {canonical_json(payload)}",
            "command": None,
        }

    return _error("unknown event kind")


def execute_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute the deliberately allow-listed local tools."""

    require_identifier(tool_name, "tool_name")
    require_payload(arguments, "arguments")
    if tool_name == "math.sum":
        if set(arguments) != {"numbers"} or not isinstance(arguments["numbers"], list):
            raise ValidationError("math.sum requires only a numbers array")
        numbers = arguments["numbers"]
        if not numbers or len(numbers) > MAX_SUM_ITEMS:
            raise ValidationError(f"numbers must contain 1-{MAX_SUM_ITEMS} values")
        normalized: list[float] = []
        for index, value in enumerate(numbers):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValidationError(f"numbers[{index}] must be numeric")
            value = float(value)
            if not math.isfinite(value):
                raise ValidationError(f"numbers[{index}] must be finite")
            normalized.append(value)
        total = math.fsum(normalized)
        return {"ok": True, "count": len(normalized), "sum": total}

    if tool_name == "text.word_count":
        if set(arguments) != {"text"} or not isinstance(arguments["text"], str):
            raise ValidationError("text.word_count requires only a text string")
        text = arguments["text"]
        if len(text.encode()) > MAX_MESSAGE_BYTES:
            raise ValidationError(f"text exceeds {MAX_MESSAGE_BYTES} UTF-8 bytes")
        return {"ok": True, "words": len(text.split()), "characters": len(text)}

    raise ValidationError("tool is not allow-listed")
