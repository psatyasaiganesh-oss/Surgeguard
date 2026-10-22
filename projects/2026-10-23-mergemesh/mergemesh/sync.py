"""Authenticated, retry-safe replication helpers."""

from __future__ import annotations

import hashlib
import hmac
import math
import random
from typing import Any, Mapping, Sequence

from .model import (
    IntegrityError,
    MAX_BODY_BYTES,
    Operation,
    ValidationError,
    canonical_json,
    require_exact_keys,
    strict_loads,
    validate_identifier,
)
from .store import RegionStore

ALGORITHM = "HMAC-SHA256"


def _validate_secret(secret: Any) -> bytes:
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValidationError("replication secrets must contain at least 32 bytes")
    return secret


def sign_operation(operation: Operation, secret: bytes) -> dict[str, Any]:
    """Create an origin-bound envelope. Secrets are never persisted."""

    secret = _validate_secret(secret)
    signed = {
        "schema": 1,
        "algorithm": ALGORITHM,
        "key_id": operation.origin,
        "operation": operation.as_dict(),
    }
    signature = hmac.new(
        secret, canonical_json(signed).encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return {**signed, "signature": signature}


def verify_envelope(
    envelope: Mapping[str, Any] | str | bytes,
    secrets: Mapping[str, bytes],
) -> Operation:
    """Verify a closed envelope schema and return the signed operation."""

    if isinstance(envelope, (str, bytes)):
        parsed = strict_loads(envelope, max_bytes=MAX_BODY_BYTES)
    elif isinstance(envelope, Mapping):
        # Canonical round-trip also rejects non-JSON values and excessive bodies.
        try:
            encoded = canonical_json(dict(envelope))
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"envelope is not valid JSON: {exc}") from exc
        parsed = strict_loads(encoded, max_bytes=MAX_BODY_BYTES)
    else:
        raise ValidationError("envelope must be an object or JSON body")
    if not isinstance(parsed, dict):
        raise ValidationError("envelope must be an object")
    require_exact_keys(
        parsed,
        required={"schema", "algorithm", "key_id", "operation", "signature"},
        label="envelope",
    )
    if parsed["schema"] != 1 or isinstance(parsed["schema"], bool):
        raise ValidationError("unsupported envelope schema")
    if parsed["algorithm"] != ALGORITHM:
        raise ValidationError("unsupported envelope algorithm")
    key_id = validate_identifier(parsed["key_id"], "key_id")
    signature = parsed["signature"]
    if (
        not isinstance(signature, str)
        or len(signature) != 64
        or any(character not in "0123456789abcdef" for character in signature)
    ):
        raise ValidationError("signature must be a lowercase SHA-256 digest")
    secret = secrets.get(key_id)
    if secret is None:
        raise IntegrityError(f"unknown replication key: {key_id}")
    secret = _validate_secret(secret)
    signed = {
        "schema": parsed["schema"],
        "algorithm": parsed["algorithm"],
        "key_id": key_id,
        "operation": parsed["operation"],
    }
    expected = hmac.new(
        secret, canonical_json(signed).encode("utf-8"), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise IntegrityError("replication signature mismatch")
    operation = Operation.from_dict(parsed["operation"])
    if operation.origin != key_id:
        raise IntegrityError("replication key is not authorized for operation origin")
    return operation


def receive_envelope(
    receiver: RegionStore,
    envelope: Mapping[str, Any] | str | bytes,
    secrets: Mapping[str, bytes],
) -> dict[str, Any]:
    operation = verify_envelope(envelope, secrets)
    return receiver.apply_remote(operation)


def deliver_batch(
    sender: RegionStore,
    receiver: RegionStore,
    secrets: Mapping[str, bytes],
    *,
    limit: int = 100,
    order: str = "forward",
    duplicate_every: int = 0,
    acknowledge: bool = True,
    random_seed: int = 0,
) -> dict[str, Any]:
    """Deliver one retryable batch and optionally simulate lost acknowledgements."""

    if sender.region == receiver.region:
        raise ValidationError("sender and receiver regions must differ")
    if order not in {"forward", "reverse", "shuffle"}:
        raise ValidationError("order must be forward, reverse, or shuffle")
    if isinstance(duplicate_every, bool) or not isinstance(duplicate_every, int):
        raise ValidationError("duplicate_every must be an integer")
    if duplicate_every < 0:
        raise ValidationError("duplicate_every cannot be negative")

    operations = sender.export_operations(receiver.region, limit=limit)
    if order == "reverse":
        operations.reverse()
    elif order == "shuffle":
        random.Random(random_seed).shuffle(operations)

    accepted = 0
    duplicates = 0
    conflict_observations = 0
    delivered_ids: list[str] = []
    for index, operation in enumerate(operations, start=1):
        secret = secrets.get(operation.origin)
        if secret is None:
            raise IntegrityError(f"missing signing secret for {operation.origin}")
        envelope = sign_operation(operation, secret)
        result = receive_envelope(receiver, envelope, secrets)
        duplicates += int(result["duplicate"])
        accepted += int(not result["duplicate"])
        conflict_observations += int(bool(result["conflicts"]))
        delivered_ids.append(operation.op_id)
        if duplicate_every and index % duplicate_every == 0:
            duplicate_result = receive_envelope(receiver, envelope, secrets)
            duplicates += int(duplicate_result["duplicate"])
            accepted += int(not duplicate_result["duplicate"])
    if acknowledge and delivered_ids:
        sender.mark_delivered(receiver.region, delivered_ids)
    return {
        "sender": sender.region,
        "receiver": receiver.region,
        "offered": len(operations),
        "accepted": accepted,
        "duplicates": duplicates,
        "conflict_observations": conflict_observations,
        "acknowledged": bool(acknowledge),
        "operation_ids": delivered_ids,
    }


def converge(
    stores: Sequence[RegionStore],
    secrets: Mapping[str, bytes],
    *,
    batch_size: int = 1000,
    max_rounds: int = 20,
) -> dict[str, Any]:
    """Run full-mesh anti-entropy until every directed queue is empty."""

    regions = [store.region for store in stores]
    if len(set(regions)) != len(regions):
        raise ValidationError("stores must have unique region IDs")
    total_accepted = 0
    total_duplicates = 0
    batches = 0
    for round_number in range(1, max_rounds + 1):
        offered_this_round = 0
        for sender in stores:
            for receiver in stores:
                if sender is receiver:
                    continue
                while True:
                    result = deliver_batch(
                        sender,
                        receiver,
                        secrets,
                        limit=batch_size,
                        order="forward",
                    )
                    batches += 1
                    offered_this_round += result["offered"]
                    total_accepted += result["accepted"]
                    total_duplicates += result["duplicates"]
                    if result["offered"] < batch_size:
                        break
        if offered_this_round == 0:
            return {
                "converged": True,
                "rounds": round_number,
                "batches": batches,
                "accepted": total_accepted,
                "duplicates": total_duplicates,
            }
    return {
        "converged": False,
        "rounds": max_rounds,
        "batches": batches,
        "accepted": total_accepted,
        "duplicates": total_duplicates,
    }


def whole_document_lww(revisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Small baseline: choose one complete document by numeric timestamp.

    This models the application consequence of whole-item winner selection; it is
    not an Azure Cosmos DB emulator.
    """

    if not revisions:
        raise ValidationError("LWW baseline requires at least one revision")
    validated: list[tuple[float, str, dict[str, Any]]] = []
    for revision in revisions:
        if not isinstance(revision, Mapping):
            raise ValidationError("LWW revision must be an object")
        require_exact_keys(
            revision,
            required={"timestamp", "region", "document"},
            label="LWW revision",
        )
        timestamp = revision["timestamp"]
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise ValidationError("LWW timestamp must be numeric")
        if not math.isfinite(float(timestamp)):
            raise ValidationError("LWW timestamp must be finite")
        region = validate_identifier(revision["region"], "LWW region")
        document = revision["document"]
        if not isinstance(document, dict):
            raise ValidationError("LWW document must be an object")
        validated.append((float(timestamp), region, dict(document)))
    timestamp, region, document = max(validated, key=lambda item: (item[0], item[1]))
    return {"timestamp": timestamp, "region": region, "document": document}
