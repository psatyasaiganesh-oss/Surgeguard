from __future__ import annotations

import platform
import tempfile
import time
from pathlib import Path
from typing import Any

from .engine import deterministic_handler, execute_tool
from .errors import SimulatedCrash
from .store import SliceQueue


DEMO_SECRET = b"slicequeue-public-demo-signing-secret-v1"


def _new_path(path: str | Path | None, name: str) -> tuple[Path, tempfile.TemporaryDirectory[str] | None]:
    if path is None:
        temporary = tempfile.TemporaryDirectory(prefix=f"slicequeue-{name}-")
        return Path(temporary.name) / f"{name}.db", temporary
    selected = Path(path)
    if selected.exists():
        raise FileExistsError(f"refusing to overwrite existing database: {selected}")
    selected.parent.mkdir(parents=True, exist_ok=True)
    return selected, None


def run_demo(path: str | Path | None = None) -> dict[str, Any]:
    database, temporary = _new_path(path, "demo")
    namespace = "demo"
    base = 1_000_000_000
    trace: list[dict[str, Any]] = []
    try:
        with SliceQueue(database, secret=DEMO_SECRET) as store:
            parent = store.launch_thread(
                namespace, "launch-parent", thread_id="parent", created_ns=base
            )
            child = store.launch_thread(
                namespace,
                "launch-child",
                thread_id="child",
                parent_thread_id="parent",
                created_ns=base + 1,
            )
            replayed_launch = store.launch_thread(
                namespace, "launch-parent", thread_id="parent", created_ns=base + 2
            )
            event = store.enqueue_event(
                namespace,
                "user-sum-1",
                "parent",
                "USER",
                {"action": "sum", "numbers": [3, 5, 8, 13]},
                created_ns=base + 10,
            )
            replayed_event = store.enqueue_event(
                namespace,
                "user-sum-1",
                "parent",
                "USER",
                {"action": "sum", "numbers": [3, 5, 8, 13]},
                created_ns=base + 11,
            )

            claim = store.claim_event(namespace, "slice-worker", current_ns=base + 20)
            assert claim is not None
            decision = deterministic_handler(
                claim["history"], {"kind": claim["kind"], "payload": claim["payload"]}
            )
            slice_ack_lost = False
            try:
                store.commit_slice(
                    namespace,
                    claim["event_id"],
                    "slice-worker",
                    claim["lease_token"],
                    claim["expected_thread_version"],
                    decision,
                    current_ns=base + 21,
                    failpoint="after_slice_commit",
                )
            except SimulatedCrash:
                slice_ack_lost = True
            trace.append({"step": "slice_commit", "ack_lost": slice_ack_lost})

            dispatch_ack_lost = False
            try:
                store.dispatch_outbox(
                    namespace,
                    current_ns=base + 30,
                    failpoint="after_dispatch_commit",
                )
            except SimulatedCrash:
                dispatch_ack_lost = True
            redispatch = store.dispatch_outbox(namespace, current_ns=base + 31)
            trace.append(
                {
                    "step": "outbox_dispatch",
                    "ack_lost": dispatch_ack_lost,
                    "duplicate_tasks_created": redispatch["dispatched"],
                }
            )

            task = store.claim_tool(namespace, "tool-worker", current_ns=base + 40)
            assert task is not None
            tool_result = execute_tool(task["tool_name"], task["arguments"])
            tool_ack_lost = False
            try:
                store.complete_tool(
                    namespace,
                    task["call_id"],
                    "tool-worker",
                    task["lease_token"],
                    tool_result,
                    current_ns=base + 41,
                    result_available_ns=base + 100,
                    failpoint="after_tool_commit",
                )
            except SimulatedCrash:
                tool_ack_lost = True
            tool_replay = store.complete_tool(
                namespace,
                task["call_id"],
                "tool-worker",
                task["lease_token"],
                tool_result,
                current_ns=base + 42,
                result_available_ns=base + 100,
            )
            delayed_claim = store.claim_event(
                namespace, "slice-worker", current_ns=base + 99
            )
            completed_result = store.run_one(
                namespace,
                "slice-worker",
                deterministic_handler,
                current_ns=base + 100,
            )
            trace.append(
                {
                    "step": "tool_completion",
                    "ack_lost": tool_ack_lost,
                    "retry_replayed": tool_replay["replayed"],
                    "claimed_before_due_time": delayed_claim is not None,
                    "result_version": completed_result["thread_version"] if completed_result else None,
                }
            )

            store.enqueue_event(
                namespace,
                "user-send-1",
                "parent",
                "USER",
                {
                    "action": "send",
                    "target_thread_id": "child",
                    "payload": {"sum": tool_result["sum"], "status": "ready"},
                },
                created_ns=base + 110,
            )
            send_result = store.run_one(
                namespace, "slice-worker", deterministic_handler, current_ns=base + 111
            )
            dispatch = store.dispatch_outbox(namespace, current_ns=base + 112)
            child_result = store.run_one(
                namespace, "child-worker", deterministic_handler, current_ns=base + 113
            )
            trace.append(
                {
                    "step": "cross_thread_message",
                    "source_version": send_result["thread_version"] if send_result else None,
                    "outbox_dispatched": dispatch["dispatched"],
                    "child_version": child_result["thread_version"] if child_result else None,
                }
            )

            store.enqueue_event(
                namespace,
                "timer-1",
                "parent",
                "TIMER",
                {"name": "follow-up"},
                created_ns=base + 120,
                available_ns=base + 200,
            )
            early_timer = store.claim_event(
                namespace, "slice-worker", current_ns=base + 199
            )
            timer_result = store.run_one(
                namespace, "slice-worker", deterministic_handler, current_ns=base + 200
            )
            store.enqueue_event(
                namespace,
                "close-child",
                "child",
                "USER",
                {"action": "close"},
                created_ns=base + 210,
            )
            close_result = store.run_one(
                namespace, "child-worker", deterministic_handler, current_ns=base + 211
            )
            trace.append(
                {
                    "step": "timer_and_close",
                    "timer_claimed_early": early_timer is not None,
                    "timer_version": timer_result["thread_version"] if timer_result else None,
                    "child_status": close_result["thread_status"] if close_result else None,
                }
            )

            integrity = store.verify_integrity(namespace)
            stats = store.stats(namespace)
            parent_state = store.get_thread(namespace, "parent")
            child_state = store.get_thread(namespace, "child")

        return {
            "project": "SliceQueue",
            "scenario": "at-least-once delivery with three lost acknowledgements",
            "threads": {"parent": parent, "child": child},
            "idempotency": {
                "launch_replayed": replayed_launch["replayed"],
                "event_replayed": replayed_event["replayed"],
                "original_queue_sequence": event["queue_sequence"],
                "replayed_queue_sequence": replayed_event["queue_sequence"],
            },
            "tool_result": tool_result,
            "trace": trace,
            "final_versions": {
                "parent": parent_state["version"],
                "child": child_state["version"],
            },
            "integrity": integrity,
            "stats": stats,
            "database_persisted_at": str(database) if path is not None else None,
        }
    finally:
        if temporary is not None:
            temporary.cleanup()
def run_benchmark(
    path: str | Path | None = None,
    *,
    threads: int = 250,
    events_per_thread: int = 2,
) -> dict[str, Any]:
    if threads < 1 or threads > 10_000:
        raise ValueError("threads must be between 1 and 10000")
    if events_per_thread < 1 or events_per_thread > 100:
        raise ValueError("events_per_thread must be between 1 and 100")
    database, temporary = _new_path(path, "benchmark")
    namespace = "benchmark"
    event_count = threads * events_per_thread
    try:
        with SliceQueue(database, secret=DEMO_SECRET) as store:
            setup_start = time.perf_counter()
            timestamp = 1_000_000
            for thread_index in range(threads):
                thread_id = f"thread-{thread_index}"
                store.launch_thread(
                    namespace,
                    f"launch-{thread_index}",
                    thread_id=thread_id,
                    created_ns=timestamp,
                )
                for event_index in range(events_per_thread):
                    store.enqueue_event(
                        namespace,
                        f"event-{thread_index}-{event_index}",
                        thread_id,
                        "USER",
                        {"action": "ping"},
                        created_ns=timestamp + event_index + 1,
                    )
                timestamp += events_per_thread + 2
            setup_seconds = time.perf_counter() - setup_start

            process_start = time.perf_counter()
            processed = 0
            while True:
                result = store.run_one(
                    namespace,
                    "benchmark-worker",
                    deterministic_handler,
                    current_ns=10_000_000_000,
                )
                if result is None:
                    break
                processed += 1
            process_seconds = time.perf_counter() - process_start
            verification_start = time.perf_counter()
            integrity = store.verify_integrity(namespace)
            verification_seconds = time.perf_counter() - verification_start
            stats = store.stats(namespace)
        return {
            "benchmark": "single-process SQLite synthetic bounded-slice throughput",
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
            },
            "workload": {"threads": threads, "events": event_count},
            "result": {
                "processed": processed,
                "setup_seconds": round(setup_seconds, 6),
                "processing_seconds": round(process_seconds, 6),
                "verification_seconds": round(verification_seconds, 6),
                "processing_events_per_second": round(processed / process_seconds, 2),
                "database_bytes": stats["database_bytes"],
                "integrity_valid": integrity["valid"],
            },
            "scope": "local synthetic measurement; not a production or AWS Infinity comparison",
            "database_persisted_at": str(database) if path is not None else None,
        }
    finally:
        if temporary is not None:
            temporary.cleanup()
