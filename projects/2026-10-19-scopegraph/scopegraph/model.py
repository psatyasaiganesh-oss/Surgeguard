"""Small, closed policy language. No executable expressions or wildcard subjects."""

import json
import math
import re


class Problem(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def require(condition, message, code="invalid_input", status=400):
    if not condition:
        raise Problem(code, message, status)


def fields(value, required, optional=()):
    require(isinstance(value, dict), "expected an object")
    keys = set(value)
    require(set(required) <= keys and keys <= set(required) | set(optional),
            "missing or unknown fields")
    return value


def identifier(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value),
            "invalid identifier")
    return value


def integer(value, minimum=0, maximum=2**53 - 1):
    require(type(value) is int and minimum <= value <= maximum, "integer out of range")
    return value


def reference(value, kinds=("folder", "document", "group")):
    require(isinstance(value, str) and value.count(":") == 1, "invalid reference")
    kind, name = value.split(":")
    require(kind in kinds, "unsupported reference type")
    identifier(name)
    return value


def subject(value):
    if isinstance(value, str) and value.endswith("#member"):
        reference(value[:-7], ("group",))
    else:
        reference(value, ("user",))
    return value


def permission(value):
    require(value in ("view", "edit", "share"), "unknown permission")
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False)


def parse_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(_):
        raise Problem("invalid_input", "non-finite JSON number")

    require(isinstance(raw, (str, bytes)) and len(raw) <= 65536, "JSON exceeds size limit")
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
        # Iterative depth cap also covers otherwise valid, deeply nested JSON.
        stack = [(value, 0)]
        while stack:
            node, depth = stack.pop()
            require(depth <= 16, "JSON exceeds nesting limit")
            require(not isinstance(node, float) or math.isfinite(node), "non-finite JSON number")
            if isinstance(node, dict):
                stack.extend((item, depth + 1) for item in node.values())
            elif isinstance(node, list):
                stack.extend((item, depth + 1) for item in node)
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise Problem("invalid_input", "invalid JSON") from exc


def operation(op):
    require(isinstance(op, dict), "invalid operation")
    action = op.get("op")
    if action in ("create", "drop"):
        fields(op, ("op", "object"))
        reference(op["object"])
    elif action in ("touch", "delete"):
        fields(op, ("op", "resource", "relation", "subject"),
               ("expires_at",) if action == "touch" else ())
        resource = reference(op["resource"])
        rel, who = op["relation"], op["subject"]
        require(isinstance(rel, str), "invalid relation")
        if resource.startswith("group:"):
            require(rel == "member", "groups only support member")
            subject(who)
        elif rel == "parent":
            reference(who, ("folder",))
        else:
            require(rel in ("owner", "editor", "viewer", "blocked"), "unknown relation")
            subject(who)
        expiry = op.get("expires_at")
        if expiry is not None:
            require(type(expiry) in (int, float) and math.isfinite(expiry)
                    and 0 < expiry <= 2**53 - 1, "invalid expiry")
    else:
        raise Problem("invalid_input", "unknown operation")
    return op
