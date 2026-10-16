"""Reproducible synthetic workload. Every invocation uses a new temporary DB."""

from concurrent.futures import ThreadPoolExecutor
import platform
import sqlite3
import statistics
import tempfile
import threading
import time

from .engine import Engine
from .errors import ConsistencyError, LeaseLost


def sample(customer_id, generation=0):
    return [{"id": f"sub_{customer_id}_{n}", "plan": f"plan_{n}",
             "amount_cents": 1000 + n * 250 + generation, "currency": "INR", "status": "active"}
            for n in range(3)]


def percentile(values, fraction):
    return sorted(values)[max(0, min(len(values)-1, int(len(values)*fraction + 0.999999)-1))]


def run_demo(*, customers=1000, writes=400, batch_size=100):
    from .model import integer
    integer(customers, "customers", 10, 10000)
    integer(writes, "writes", 1, 10000)
    integer(batch_size, "batch_size", 2, min(1000, customers-1))
    with tempfile.TemporaryDirectory(prefix="shiftsafe-") as folder:
        path = f"{folder}/migration.db"
        # A controllable clock makes lease-expiry injection deterministic, with no sleeps.
        clock = [1000.0]
        engine = Engine(path, clock=lambda: clock[0])
        seed_start = time.perf_counter()
        for cid in range(1, customers + 1):
            engine.put(cid, sample(cid), operation_id=f"seed:{cid}")
        seed_seconds = time.perf_counter() - seed_start
        engine.start()
        lease_a = engine.acquire("worker-a", ttl=10)
        first = engine.capture(lease_a, batch_size=batch_size)
        # The snapshot was captured BEFORE these newer writes and a tombstone.
        engine.put(1, sample(1, 99), operation_id="race:update")
        engine.delete(2, operation_id="race:delete")
        stale_result = engine.apply(lease_a, first)
        assert stale_result["skipped_newer_or_equal"] == 2
        assert engine.get(1)["subscriptions"] == sample(1, 99)
        assert engine.get(2)["deleted"]
        abandoned = engine.capture(lease_a, batch_size=batch_size)
        checkpoint_before_restart = engine.status()["cursor"]
        clock[0] += 11
        restarted = Engine(path, clock=lambda: clock[0])
        lease_b = restarted.acquire("worker-b", ttl=300)
        try:
            restarted.apply(lease_a, abandoned)
        except LeaseLost:
            old_worker_fenced = True
        else:
            raise AssertionError("expired worker was allowed to commit")
        checkpoint_survived = restarted.status()["cursor"] == checkpoint_before_restart
        latencies = []
        rounds = (customers - checkpoint_before_restart + batch_size - 1) // batch_size
        burst_barrier = threading.Barrier(2, timeout=15)

        def writer():
            worker = Engine(path, clock=lambda: clock[0])
            index = 0
            for round_index in range(rounds):
                burst_barrier.wait()
                burst_size = writes // rounds + int(round_index < writes % rounds)
                for _ in range(burst_size):
                    cid = 3 + (index * 37) % (customers - 2)
                    begin = time.perf_counter()
                    if index % 13 == 0:
                        worker.delete(cid, operation_id=f"load:{index}")
                    else:
                        worker.put(cid, sample(cid, index), operation_id=f"load:{index}")
                    latencies.append((time.perf_counter() - begin) * 1000)
                    index += 1
                burst_barrier.wait()

        load_start = time.perf_counter()
        batches, scanned = 1, stale_result["scanned"]
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(writer)
            try:
                for round_index in range(rounds):
                    batch = restarted.capture(lease_b, batch_size=batch_size)
                    burst_barrier.wait()
                    # Hold the last checkpoint until its writer burst completes.
                    # Thus every load write finishes before backfill completion.
                    if round_index == rounds-1:
                        burst_barrier.wait()
                    result = restarted.apply(lease_b, batch)
                    if round_index != rounds-1:
                        burst_barrier.wait()
                    batches += 1
                    scanned += result["scanned"]
            except BaseException:
                burst_barrier.abort()
                raise
            future.result(timeout=30)
        assert restarted.status()["backfill_complete"]
        load_seconds = time.perf_counter() - load_start
        restarted.release(lease_b)
        initial_verification = restarted.verify()
        assert initial_verification["ok"]
        restarted.shadow()
        # SQL is used only by this trusted fault harness, never an application path.
        with sqlite3.connect(path) as con:
            row = con.execute("SELECT customer_id,subscription_id FROM subscriptions_v2 ORDER BY customer_id LIMIT 1").fetchone()
            con.execute("UPDATE subscriptions_v2 SET amount_cents=amount_cents+1 WHERE customer_id=? AND subscription_id=?", row)
        shadow = restarted.get(row[0])
        assert shadow["source"] == "legacy" and shadow["shadow_match"] is False
        try:
            restarted.cutover()
        except ConsistencyError as exc:
            blocked = exc.report
        else:
            raise AssertionError("corrupt projection passed cutover")
        restarted.repair(row[0])
        after_repair = restarted.cutover()
        assert after_repair["ok"] and after_repair["mismatches"] == 0
        restarted.put(1, sample(1, 777), operation_id="new:write")
        assert restarted.get(1)["source"] == "new"
        restarted.rollback()
        rollback_preserved_new_write = restarted.get(1)["subscriptions"] == sample(1, 777)
        restarted.shadow()
        restarted.cutover()
        final_verification = restarted.finalize()
        removed = 0
        while True:
            count = restarted.cleanup(batch_size=batch_size)
            removed += count
            if count == 0:
                break
        request = sample(1, 888)
        receipt = restarted.put(1, request, operation_id="final:write")
        replay = restarted.put(1, request, operation_id="final:write")
        assert replay["replayed"] and receipt["revision"] == replay["revision"]
        final_status = restarted.status()
        assert final_status["legacy_customers"] == 0 and removed == customers
        assert restarted.get(1)["subscriptions"] == request
        return {
            "success": all([checkpoint_survived, old_worker_fenced, rollback_preserved_new_write, final_verification["ok"]]),
            "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                            "platform": platform.system(), "storage": "temporary local SQLite, WAL, synchronous=FULL"},
            "workload": {"seed_customers": customers, "subscriptions_per_customer": 3,
                         "concurrent_writer_operations": writes, "batch_size": batch_size,
                         "writes_completed_while_backfill_active": writes,
                         "scheduling": "one writer burst per captured batch; final checkpoint waits for last burst"},
            "backfill": {"batches": batches, "source_rows_scanned": scanned,
                         "stale_snapshot_rows_skipped": stale_result["skipped_newer_or_equal"],
                         "restart_checkpoint": checkpoint_before_restart, "checkpoint_survived_restart": checkpoint_survived,
                         "expired_worker_fenced": old_worker_fenced, "final_fence": lease_b.fence},
            "safety": {"corruption_mismatches": blocked["mismatches"], "corrupt_cutover_blocked": True,
                       "shadow_read_preserved_legacy": True, "mismatches_after_repair": after_repair["mismatches"],
                       "rollback_preserved_new_write": rollback_preserved_new_write,
                       "idempotent_replay_revision_unchanged": True, "legacy_rows_removed": removed},
            "timings": {"seed_seconds": round(seed_seconds, 6), "concurrent_load_seconds": round(load_seconds, 6),
                        "writes_per_second_during_load": round(writes/load_seconds, 2),
                        "writer_latency_ms_p50": round(statistics.median(latencies), 3),
                        "writer_latency_ms_p95": round(percentile(latencies, 0.95), 3),
                        "writer_latency_ms_max": round(max(latencies), 3)},
            "verified_before_finalization": final_verification,
            "final_status": final_status,
            "limits": "One local run; timing varies with hardware and scheduling. Not Stripe performance or proof of production zero downtime."
        }
