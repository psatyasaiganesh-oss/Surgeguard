"""Closed input schema and exact, independently testable burn-rate policy math."""

import json
import math
import re
from fractions import Fraction

MINUTE = 60
MAX_HISTORY_MINUTES = 4320


class Problem(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def require(condition, message, code="invalid_input", status=400):
    if not condition:
        raise Problem(code, message, status)


def fields(value, required, optional=()):
    require(type(value) is dict, "expected an object")
    require(set(required) <= set(value) <= set(required) | set(optional), "missing or unknown fields")
    return value


def identifier(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value),
            "invalid identifier")
    return value


def integer(value, low=0, high=2**53 - 1):
    require(type(value) is int and low <= value <= high, "integer out of range")
    return value


def minute(value):
    integer(value, 0, 4102444800)
    require(value % MINUTE == 0, "time must be a Unix second aligned to a minute")
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def parse_json(raw):
    require(isinstance(raw, (bytes, str)) and len(raw) <= 65536, "JSON exceeds 64 KiB")

    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise Problem("invalid_input", "non-finite JSON number")

    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
        stack = [(value, 0)]
        while stack:
            node, depth = stack.pop()
            require(depth <= 12, "JSON nesting limit exceeded")
            require(not isinstance(node, float) or math.isfinite(node), "non-finite JSON number")
            if isinstance(node, dict):
                stack.extend((item, depth + 1) for item in node.values())
            elif isinstance(node, list):
                stack.extend((item, depth + 1) for item in node)
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise Problem("invalid_input", "invalid JSON") from exc


def rate(text):
    require(isinstance(text, str) and re.fullmatch(r"[0-9]{1,6}(\.[0-9]{1,6})?", text),
            "burn threshold must be a bounded decimal string")
    result = Fraction(text)
    require(result > 0, "burn threshold must be positive")
    return result


def default_policy():
    return {"target_ppm": 999000, "period_minutes": 43200, "stale_after_seconds": 120,
            "rules": [
                {"name": "fast", "severity": "PAGE", "long_minutes": 60, "short_minutes": 5,
                 "burn": "14.4", "min_requests": 100},
                {"name": "slow", "severity": "PAGE", "long_minutes": 360, "short_minutes": 30,
                 "burn": "6", "min_requests": 100},
                {"name": "ticket", "severity": "TICKET", "long_minutes": 4320, "short_minutes": 360,
                 "burn": "1", "min_requests": 100},
            ]}


def validate_policy(policy):
    fields(policy, ("target_ppm", "period_minutes", "stale_after_seconds", "rules"))
    integer(policy["target_ppm"], 1, 999999)
    integer(policy["period_minutes"], 1, 43200)
    integer(policy["stale_after_seconds"], 60, 3600)
    rules = policy["rules"]
    require(isinstance(rules, list) and 1 <= len(rules) <= 8, "expected 1..8 rules")
    names = set()
    for rule in rules:
        fields(rule, ("name", "severity", "long_minutes", "short_minutes", "burn", "min_requests"))
        identifier(rule["name"])
        require(rule["name"] not in names, "duplicate rule name")
        names.add(rule["name"])
        require(rule["severity"] in ("PAGE", "TICKET"), "invalid severity")
        integer(rule["long_minutes"], 1, min(MAX_HISTORY_MINUTES, policy["period_minutes"]))
        integer(rule["short_minutes"], 1, rule["long_minutes"])
        integer(rule["min_requests"], 1, 10**9)
        # Strict > is used below. Reject a threshold that cannot fire at 100% errors.
        require(rate(rule["burn"]) < Fraction(1000000, 1000000 - policy["target_ppm"]),
                "burn threshold is unreachable for this SLO")
    return policy


def bucket(value):
    fields(value, ("minute", "total", "bad"))
    minute(value["minute"])
    integer(value["total"], 0, 10**9)
    integer(value["bad"], 0, value["total"])
    return value


def assess(policy, start, end, windows, previous, stale):
    """Only READY evidence may change a rule latch; unknown evidence holds it."""
    active = dict(previous)
    details = []
    budget_ppm = 1000000 - policy["target_ppm"]
    for rule in policy["rules"]:
        long = windows[rule["long_minutes"]]
        short = windows[rule["short_minutes"]]
        if stale:
            quality = "STALE"
        elif end - start < rule["long_minutes"] * MINUTE:
            quality = "WARMING"
        elif any(window["minutes"] != length or window["complete_minutes"] != length
                 for window, length in ((long, rule["long_minutes"]), (short, rule["short_minutes"]))):
            quality = "INCOMPLETE"
        elif not long["total"] or not short["total"]:
            quality = "NO_TRAFFIC"
        elif min(long["total"], short["total"]) < rule["min_requests"]:
            quality = "LOW_TRAFFIC"
        else:
            quality = "READY"
        threshold = rate(rule["burn"])

        def exceeds(window):
            return (window["bad"] * 1000000 * threshold.denominator
                    > window["total"] * budget_ppm * threshold.numerator)

        firing = quality == "READY" and exceeds(long) and exceeds(short)
        if quality == "READY":
            active[rule["name"]] = firing
        else:
            active.setdefault(rule["name"], False)

        def describe(window):
            return {**window, "error_ratio": window["bad"] / window["total"] if window["total"] else None,
                    "burn_rate": float(Fraction(window["bad"] * 1000000, window["total"] * budget_ppm))
                    if window["total"] else None}

        details.append({"name": rule["name"], "severity": rule["severity"], "quality": quality,
                        "threshold": rule["burn"], "firing_now": firing,
                        "active": active[rule["name"]], "long": describe(long), "short": describe(short)})
    qualities = {item["quality"] for item in details}
    quality = next((item for item in ("STALE", "INCOMPLETE", "NO_TRAFFIC", "LOW_TRAFFIC", "WARMING")
                    if item in qualities), "COMPLETE")
    severity = "PAGE" if any(item["active"] and item["severity"] == "PAGE" for item in details) else (
        "TICKET" if any(item["active"] for item in details) else "CLEAR")
    holding = any(item["active"] and item["quality"] != "READY" for item in details)
    status = (severity + ("_HOLD" if holding else "")) if severity != "CLEAR" else (
        "OK" if quality == "COMPLETE" else quality)
    return {"severity": severity, "quality": quality, "holding": holding, "status": status,
            "rules": details, "active": active}
