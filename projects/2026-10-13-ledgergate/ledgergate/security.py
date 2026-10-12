"""Stripe-compatible webhook signature primitives for the local lab.

Production Stripe integrations should use Stripe's official SDK. This small
implementation exists so the project remains credential-free and testable.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Iterable


class SignatureError(ValueError):
    """A signature header is malformed, invalid, or outside the time window."""


def _validate_secret(secret: bytes) -> None:
    if not isinstance(secret, bytes) or len(secret) < 16:
        raise ValueError("webhook secrets must be bytes containing at least 16 bytes")


def sign_payload(payload: bytes, secret: bytes, timestamp: int | None = None) -> str:
    """Create a test header using the documented `t=...,v1=...` format."""
    if not isinstance(payload, bytes):
        raise TypeError("payload must be raw bytes")
    _validate_secret(secret)
    timestamp = int(time.time()) if timestamp is None else timestamp
    if type(timestamp) is not int or timestamp < 0:
        raise ValueError("timestamp must be a nonnegative integer")
    message = str(timestamp).encode("ascii") + b"." + payload
    digest = hmac.new(secret, message, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify_signature(
    payload: bytes,
    header: str,
    secrets: Iterable[bytes],
    *,
    now: int | None = None,
    tolerance_seconds: int = 300,
) -> int:
    """Return the signed timestamp after constant-time verification.

    Multiple v1 signatures and multiple active secrets support secret
    rotation. Timestamp validation happens after signature validation so a
    caller cannot use this function as an unsigned clock oracle.
    """
    if not isinstance(payload, bytes):
        raise TypeError("payload must be raw bytes")
    if not isinstance(header, str) or not header or len(header) > 4096:
        raise SignatureError("missing or oversized signature header")
    if type(tolerance_seconds) is not int or tolerance_seconds <= 0:
        raise ValueError("tolerance_seconds must be a positive integer")
    secret_list = tuple(secrets)
    if not 1 <= len(secret_list) <= 8:
        raise ValueError("provide between one and eight active secrets")
    for secret in secret_list:
        _validate_secret(secret)

    timestamps: list[str] = []
    candidates: list[str] = []
    for component in header.split(","):
        key, separator, value = component.strip().partition("=")
        if not separator:
            raise SignatureError("malformed signature header")
        if key == "t":
            timestamps.append(value)
        elif key == "v1":
            candidates.append(value.lower())
    if len(timestamps) != 1 or not timestamps[0].isascii() or not timestamps[0].isdigit():
        raise SignatureError("signature header must contain exactly one integer timestamp")
    if not 1 <= len(candidates) <= 16:
        raise SignatureError("signature header must contain between one and sixteen v1 signatures")
    timestamp_text = timestamps[0]
    if len(timestamp_text) > 12:
        raise SignatureError("timestamp is out of range")
    timestamp = int(timestamp_text)
    if any(len(candidate) != 64 or any(char not in "0123456789abcdef" for char in candidate) for candidate in candidates):
        raise SignatureError("v1 signatures must be 64 lowercase hexadecimal characters")

    signed = timestamp_text.encode("ascii") + b"." + payload
    matched = False
    # Do not short-circuit comparisons: process every active rotation secret
    # and signature candidate to keep the comparison path less data-dependent.
    for secret in secret_list:
        expected = hmac.new(secret, signed, hashlib.sha256).hexdigest()
        for candidate in candidates:
            matched = hmac.compare_digest(expected, candidate) or matched
    if not matched:
        raise SignatureError("signature verification failed")

    current = int(time.time()) if now is None else now
    if type(current) is not int or current < 0:
        raise ValueError("now must be a nonnegative integer")
    if abs(current - timestamp) > tolerance_seconds:
        raise SignatureError("signed timestamp is outside the allowed tolerance")
    return timestamp
