"""Authenticated, retry-safe invalidation delivery."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Mapping

from .cache import CacheStore
from .model import (
    AuthorizationError,
    InvalidationEvent,
    ValidationError,
    canonical_json,
    require_keys,
    strict_loads,
    validate_digest,
    validate_id,
)
from .origin import OriginStore


def _validate_secret(secret: bytes) -> bytes:
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValidationError("HMAC secrets must contain at least 32 bytes")
    return secret


def sign_event(event: InvalidationEvent, *, key_id: str, secret: bytes) -> str:
    event.validate()
    key_id = validate_id(key_id, "key_id")
    secret = _validate_secret(secret)
    event_json = event.to_json()
    material = f"cachefence:v1:{key_id}:{event_json}".encode("utf-8")
    signature = hmac.new(secret, material, hashlib.sha256).hexdigest()
    return canonical_json(
        {
            "schema": 1,
            "key_id": key_id,
            "event": event.as_dict(),
            "signature": signature,
        }
    )


def verify_envelope(data: str | bytes, *, keys: Mapping[str, bytes]) -> InvalidationEvent:
    value = strict_loads(data)
    if not isinstance(value, dict):
        raise ValidationError("signed envelope must be an object")
    require_keys(
        value,
        required={"schema", "key_id", "event", "signature"},
        label="signed envelope",
    )
    if value["schema"] != 1 or isinstance(value["schema"], bool):
        raise ValidationError("unsupported signed envelope schema")
    key_id = validate_id(value["key_id"], "key_id")
    signature = validate_digest(value["signature"], "signature")
    if key_id not in keys:
        raise AuthorizationError("unknown signing key")
    secret = _validate_secret(keys[key_id])
    event = InvalidationEvent.from_dict(value["event"])
    event_json = event.to_json()
    material = f"cachefence:v1:{key_id}:{event_json}".encode("utf-8")
    expected = hmac.new(secret, material, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise AuthorizationError("invalid event signature")
    if key_id != event.origin:
        raise AuthorizationError("signing key does not match event origin")
    return event


@dataclass(frozen=True)
class DeliverySummary:
    claimed: int
    applied: int
    duplicates: int
    outdated: int
    acknowledged: int
    failed: int

    def as_dict(self) -> dict[str, int]:
        return {
            "claimed": self.claimed,
            "applied": self.applied,
            "duplicates": self.duplicates,
            "outdated": self.outdated,
            "acknowledged": self.acknowledged,
            "failed": self.failed,
        }


def deliver_once(
    origin: OriginStore,
    cache: CacheStore,
    *,
    secret: bytes,
    worker: str = "worker-a",
    lease_ns: int = 5_000_000_000,
    limit: int = 100,
    max_attempts: int = 8,
    now_ns: int | None = None,
    acknowledge: bool = True,
) -> DeliverySummary:
    """Claim, authenticate, apply, then acknowledge a batch.

    ``acknowledge=False`` is a deterministic crash/lost-ACK simulation: the cache
    commit survives and the lease later expires, exercising duplicate delivery.
    """
    secret = _validate_secret(secret)
    claims = origin.claim_events(
        worker,
        lease_ns=lease_ns,
        limit=limit,
        max_attempts=max_attempts,
        now_ns=now_ns,
    )
    counts = {"applied": 0, "duplicates": 0, "outdated": 0, "acknowledged": 0, "failed": 0}
    for claim in claims:
        try:
            envelope = sign_event(claim.event, key_id=origin.origin_id, secret=secret)
            received = verify_envelope(envelope, keys={origin.origin_id: secret})
            result = cache.apply_invalidation(received, now_ns=now_ns)
            if result.status == "duplicate":
                counts["duplicates"] += 1
            elif result.status == "outdated":
                counts["outdated"] += 1
            else:
                counts["applied"] += 1
            if acknowledge:
                origin.ack_event(claim.event.event_id, claim.token, now_ns=now_ns)
                counts["acknowledged"] += 1
        except Exception as exc:
            counts["failed"] += 1
            try:
                origin.fail_event(
                    claim.event.event_id,
                    claim.token,
                    type(exc).__name__,
                    max_attempts=max_attempts,
                    now_ns=now_ns,
                )
            except Exception:
                # The original exception is the useful signal; a lease may have
                # expired before failure bookkeeping could be committed.
                pass
    return DeliverySummary(claimed=len(claims), **counts)
