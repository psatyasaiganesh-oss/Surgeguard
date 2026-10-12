"""Deterministic experiments for duplicates, reordering, replay, and crashes."""

from __future__ import annotations

import json
import platform
import random
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from .schema import parse_event
from .security import SignatureError, sign_payload, verify_signature
from .service import LedgerGate
from .store import SQLiteStore


SECRET = b"whsec_local_demo_secret_32_bytes!!"


def payload(event_id: str, event_type: str, payment_id: str, version: int, status: str, amount: int, created: int) -> bytes:
    return json.dumps(
        {
            "id": event_id,
            "type": event_type,
            "created": created,
            "data": {"object": {"id": payment_id, "version": version, "status": status, "amount": amount, "currency": "usd"}},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def build_deliveries(count: int, seed: int, now: int):
    rng = random.Random(seed)
    deliveries: list[tuple[bytes, str, str]] = []
    expected: dict[str, str] = {}
    final_raw: list[bytes] = []
    for index in range(count):
        payment_id = f"pi_demo_{index:06d}"
        amount = 1_000 + index
        created = payload(f"evt_created_{index:06d}", "payment_intent.created", payment_id, 1, "created", amount, now)
        succeeded = index % 4 != 0
        final_type = "payment_intent.succeeded" if succeeded else "payment_intent.payment_failed"
        final_status = "succeeded" if succeeded else "failed"
        final = payload(f"evt_final_{index:06d}", final_type, payment_id, 2, final_status, amount, now)
        expected[payment_id] = final_status
        final_raw.append(final)
        deliveries.extend([(created, sign_payload(created, SECRET, now), "base"), (final, sign_payload(final, SECRET, now), "base")])

        # Same event object delivered again.
        if index % 5 == 0:
            deliveries.append((final, sign_payload(final, SECRET, now), "exact_duplicate"))
        # Distinct event object describing the same logical transition.
        if index % 7 == 0:
            semantic = payload(f"evt_semantic_{index:06d}", final_type, payment_id, 2, final_status, amount, now)
            deliveries.append((semantic, sign_payload(semantic, SECRET, now), "semantic_duplicate"))

    # Validly-shaped traffic with invalid authentication.
    for index, raw in enumerate(final_raw[: max(1, count // 20)]):
        deliveries.append((raw, f"t={now},v1={'0' * 64}", "invalid_signature"))
    # A genuine signature copied outside its allowed time window.
    old = now - 600
    for raw in final_raw[max(1, count // 20) : max(2, count // 10)]:
        deliveries.append((raw, sign_payload(raw, SECRET, old), "stale_replay"))
    rng.shuffle(deliveries)
    return deliveries, expected


def naive_process(deliveries, expected, now):
    state = {}
    fulfillments = 0
    authenticated = 0
    for raw, signature, _ in deliveries:
        try:
            verify_signature(raw, signature, [SECRET], now=now)
        except SignatureError:
            continue
        authenticated += 1
        event = parse_event(raw)
        # A common bug: every delivery overwrites current state and every
        # success repeats a side effect.
        state[event.payment_id] = event.status
        if event.status == "succeeded":
            fulfillments += 1
    mismatches = sum(state.get(payment_id) != status for payment_id, status in expected.items())
    expected_fulfillments = sum(status == "succeeded" for status in expected.values())
    return {
        "authenticated_deliveries": authenticated,
        "final_state_mismatches": mismatches,
        "fulfillment_attempts": fulfillments,
        "expected_unique_fulfillments": expected_fulfillments,
        "duplicate_fulfillment_attempts": fulfillments - expected_fulfillments,
    }


def crash_recovery(now: int) -> dict:
    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteStore(Path(directory) / "crash.db")
        service = LedgerGate(store, [SECRET])
        raw = payload("evt_crash_0001", "payment_intent.succeeded", "pi_crash_0001", 2, "succeeded", 5_000, now)
        receipt = service.accept(raw, sign_payload(raw, SECRET, now), now=now)
        try:
            store.process_one(now=now, crash_before_commit=True)
        except RuntimeError:
            pass
        after_inbox_crash = store.snapshot()
        store.process_one(now=now)
        try:
            store.dispatch_one(now=now, lease_seconds=0, crash_after_sink=True)
        except RuntimeError:
            pass
        after_sink_crash = store.snapshot()
        store.dispatch_one(now=now, lease_seconds=0)
        recovered = store.snapshot()
        payment = store.payment("pi_crash_0001")
        store.close()
    checks = {
        "inbox_rollback_left_event_pending": after_inbox_crash["events"].get("pending") == 1 and after_inbox_crash["payments"] == 0,
        "sink_crash_created_one_unique_effect": after_sink_crash["sink_unique_deliveries"] == 1 and after_sink_crash["outbox"].get("inflight") == 1,
        "retry_did_not_duplicate_effect": recovered["sink_unique_deliveries"] == 1 and recovered["sink_delivery_attempts"] == 2,
        "outbox_eventually_delivered": recovered["outbox"].get("delivered") == 1,
        "payment_projected": payment is not None and payment["status"] == "succeeded",
        "intake_accepted": receipt.result == "accepted",
    }
    return {"checks": checks, "after_inbox_crash": after_inbox_crash, "after_sink_crash": after_sink_crash, "recovered": recovered}


def run_demo(count: int = 400, seed: int = 13) -> dict:
    if type(count) is not int or not 10 <= count <= 20_000:
        raise ValueError("count must be an integer in [10, 20000]")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    now = 1_800_000_000
    deliveries, expected = build_deliveries(count, seed, now)
    baseline_started = time.perf_counter()
    baseline = naive_process(deliveries, expected, now)
    baseline["elapsed_seconds"] = round(time.perf_counter() - baseline_started, 6)

    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteStore(Path(directory) / "ledgergate.db")
        service = LedgerGate(store, [SECRET])
        started = time.perf_counter()
        intake = {"accepted": 0, "duplicate_event": 0, "duplicate_effect": 0, "event_conflict": 0, "invalid_signature": 0, "invalid_payload": 0}
        for raw, signature, _ in deliveries:
            receipt = service.accept(raw, signature, now=now)
            intake[receipt.result] += 1
        drain = service.drain(now=now)
        elapsed = time.perf_counter() - started
        payments = {row["payment_id"]: row["status"] for row in store.payments()}
        robust = {
            "elapsed_seconds": round(elapsed, 6),
            "intake": intake,
            "final_state_mismatches": sum(payments.get(key) != value for key, value in expected.items()),
            "projected_payments": len(payments),
            "expected_payments": count,
            "unique_fulfillments": store.snapshot()["sink_unique_deliveries"],
            "expected_unique_fulfillments": sum(value == "succeeded" for value in expected.values()),
            "drain": drain,
        }
        store.close()

    recovery = crash_recovery(now)
    checks = {
        "naive_baseline_exhibits_state_corruption": baseline["final_state_mismatches"] > 0,
        "naive_baseline_duplicates_fulfillment": baseline["duplicate_fulfillment_attempts"] > 0,
        "ledgergate_projects_every_payment": robust["projected_payments"] == count,
        "ledgergate_final_state_exact": robust["final_state_mismatches"] == 0,
        "ledgergate_fulfills_success_once": robust["unique_fulfillments"] == robust["expected_unique_fulfillments"],
        "invalid_and_replayed_signatures_rejected": robust["intake"]["invalid_signature"] == max(1, count // 20) + max(1, count // 20),
        "duplicates_detected": robust["intake"]["duplicate_event"] > 0 and robust["intake"]["duplicate_effect"] > 0,
        "crash_recovery_checks_pass": all(recovery["checks"].values()),
    }
    if not all(checks.values()):
        raise AssertionError(f"demo invariant failed: {checks}")
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(),
        "platform": platform.system(),
        "scope": "Synthetic local experiment; not Stripe traffic or a production benchmark",
        "seed": seed,
        "payments": count,
        "deliveries_offered": len(deliveries),
        "sources": [
            "https://docs.stripe.com/webhooks",
            "https://docs.stripe.com/api/idempotent_requests",
        ],
        "naive_authenticated_baseline": baseline,
        "ledgergate": robust,
        "crash_recovery": recovery,
        "invariants": checks,
    }
