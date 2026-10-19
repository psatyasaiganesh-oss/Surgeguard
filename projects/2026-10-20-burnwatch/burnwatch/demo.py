"""Synthetic telemetry, real collector/storage/HTTP operations, and measured timings."""

import http.client
import json
import platform
import sqlite3
import statistics
import tempfile
import threading
import time
from pathlib import Path

from .delivery import Inbox, deliver
from .engine import Engine
from .http import Server
from .model import Problem, default_policy

EPOCH = 2000000040


class Clock:
    def __init__(self, now=EPOCH):
        self.now = now

    def __call__(self):
        return self.now


def feed(engine, clock, service, start, minutes, phase, *, producers=("api-a", "api-b"), total=1000, bad=0):
    end = start + minutes * 60
    assert end >= clock.now, "synthetic scenario clock must not move backwards"
    clock.now = end
    last = None
    for producer in producers:
        for offset in range(0, minutes, 500):
            count = min(500, minutes - offset)
            values = [{"minute": start + i * 60, "total": total, "bad": bad}
                      for i in range(offset, offset + count)]
            last = engine.ingest(service, producer, f"{phase}-{producer}-{offset}", list(reversed(values)),
                                 start + (offset + count) * 60)
    return end, last


def benchmark(path):
    clock = Clock()
    engine = Engine(path, clock=clock)
    start = EPOCH - 5000 * 60
    engine.register("bench", ["api-a", "api-b"], default_policy(), start)
    began = time.perf_counter()
    feed(engine, clock, "bench", start, 5000, "bench")
    ingest_seconds = time.perf_counter() - began
    values = []
    for _ in range(200):
        began = time.perf_counter()
        result = engine.evaluate("bench")
        values.append((time.perf_counter() - began) * 1000)
        assert result["status"] == "OK"
    before = result["rules"]
    pruned = engine.prune("bench")
    assert engine.evaluate("bench")["rules"] == before
    assert engine.verify("bench")["passed"]
    return {"ingested_minute_reports": 10000, "ingest_batches": 20,
            "ingest_seconds": round(ingest_seconds, 6),
            "minute_reports_per_second": round(10000 / ingest_seconds, 2),
            "evaluations": len(values), "evaluation_p50_ms": round(statistics.median(values), 4),
            "evaluation_p95_ms": round(sorted(values)[int((len(values) - 1) * .95)], 4),
            "pruning": pruned, "pruning_preserved_evaluation": True,
            "scope": "sequential local SQLite operations; synthetic minute aggregates, not live user requests"}


def http_demo(engine):
    statuses = {}
    with Server(engine) as server:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        thread.start()
        token = engine.mint("checkout", "api-a")
        try:
            for name, method, route, body, key, expected in [
                ("status", "GET", "/v1/status", None, token, 200),
                ("evaluate", "POST", "/v1/evaluate", {}, token, 200),
                ("invalid_key", "GET", "/v1/status", None, "bw_" + "0" * 64, 401),
                ("identity_override", "POST", "/v1/buckets", {"batch_id": "evil", "buckets": [],
                                                              "producer": "api-b"}, token, 400),
            ]:
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    conn.request(method, route, json.dumps(body) if body is not None else None,
                                 {"Authorization": "Bearer " + key, "Content-Type": "application/json"})
                    response = conn.getresponse()
                    statuses[name] = response.status
                    response.read()
                    assert response.status == expected
                finally:
                    conn.close()
        finally:
            server.shutdown()
            thread.join(5)
    return statuses


def run():
    with tempfile.TemporaryDirectory(prefix="burnwatch-demo-") as folder:
        clock = Clock()
        engine = Engine(Path(folder) / "telemetry.db", clock=clock)
        policy = default_policy()
        start = EPOCH - 4320 * 60
        engine.register("checkout", ["api-a", "api-b"], policy, start)
        end, _ = feed(engine, clock, "checkout", start, 4320, "warm")
        assert engine.evaluate("checkout")["status"] == "OK"
        end, _ = feed(engine, clock, "checkout", end, 1, "brief", bad=10)
        transient = engine.evaluate("checkout")
        naive = transient["rules"][0]["short"]["error_ratio"] > .001
        assert naive and transient["severity"] == "CLEAR"
        end, _ = feed(engine, clock, "checkout", end, 5, "settle")
        engine.evaluate("checkout")
        outage_start = end
        detection = None
        for index in range(1, 6):
            end, _ = feed(engine, clock, "checkout", end, 1, f"outage-{index}", bad=200)
            result = engine.evaluate("checkout")
            if result["severity"] == "PAGE" and detection is None:
                detection = index
        assert detection == 5
        end, _ = feed(engine, clock, "checkout", end, 25, "sustained", bad=200)
        outage = engine.evaluate("checkout")
        assert sum(item["firing_now"] for item in outage["rules"]) == 3
        severity_events = [event for event in engine.pending() if event["kind"] == "severity"]
        assert sum(event["to"]["severity"] == "PAGE" for event in severity_events) == 1

        outage_end = end
        end, _ = feed(engine, clock, "checkout", end, 3, "stalled-b", producers=("api-a",))
        held = engine.evaluate("checkout")
        assert held["status"] == "PAGE_HOLD" and held["quality"] == "STALE"
        feed(engine, clock, "checkout", outage_end, 3, "catchup-b", producers=("api-b",))
        engine.evaluate("checkout")
        end, _ = feed(engine, clock, "checkout", end, 1, "recovery-4")
        assert engine.evaluate("checkout")["rules"][0]["firing_now"]
        end, _ = feed(engine, clock, "checkout", end, 1, "recovery-5")
        reset = engine.evaluate("checkout")
        assert not reset["rules"][0]["firing_now"] and reset["rules"][0]["long"]["burn_rate"] > 14.4
        end, _ = feed(engine, clock, "checkout", end, 25, "recovery-30")
        assert engine.evaluate("checkout")["severity"] == "TICKET"
        end, _ = feed(engine, clock, "checkout", end, 25, "recovery-55")
        assert engine.evaluate("checkout")["rules"][0]["long"]["burn_rate"] > 14.4
        end, _ = feed(engine, clock, "checkout", end, 1, "recovery-56")
        assert engine.evaluate("checkout")["rules"][0]["long"]["burn_rate"] <= 14.4
        end, _ = feed(engine, clock, "checkout", end, 302, "recovery-358")
        assert engine.evaluate("checkout")["severity"] == "TICKET"
        end, _ = feed(engine, clock, "checkout", end, 1, "recovery-359")
        assert engine.evaluate("checkout")["status"] == "OK"

        # Exact batch retries do not duplicate aggregate counts, even after sealing.
        values = [{"minute": outage_start, "total": 1000, "bad": 200}]
        replay = engine.ingest("checkout", "api-a", "outage-1-api-a-0", values, outage_start + 60)
        assert replay["replayed"] and engine.verify("checkout")["passed"]

        inbox = Inbox(Path(folder) / "inbox.db")

        def crash(_):
            raise RuntimeError("synthetic crash after durable inbox write, before acknowledgement")

        try:
            deliver(engine, inbox, _after_store=crash)
        except RuntimeError:
            pass
        delivered = deliver(engine, inbox)
        assert delivered["duplicate_deliveries"] == 1 and not engine.pending()
        events = inbox.events()
        assert len(events) == len({event["event_id"] for event in events})

        quiet = default_policy()
        quiet["rules"] = quiet["rules"][:1]
        quiet_start = clock.now - 60 * 60
        engine.register("quiet", ["api-a"], quiet, quiet_start)
        feed(engine, clock, "quiet", quiet_start, 60, "quiet", producers=("api-a",), total=0)
        no_traffic = engine.evaluate("quiet")
        assert no_traffic["status"] == "NO_TRAFFIC"
        statuses = http_demo(engine)
        extra = deliver(engine, inbox)
        for key in ("attempted", "new_inbox_events", "duplicate_deliveries"):
            delivered[key] += extra[key]
        events = inbox.events()
        assert not engine.pending()
        timings = benchmark(Path(folder) / "benchmark.db")
        return {"project": "BurnWatch", "implementation": "independent Google SRE-inspired local alerting pipeline",
                "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                                "system": platform.system()},
                "scenario": {"warmup_minutes": 4320, "producers": 2, "brief_spike_naive_threshold_would_page": naive,
                             "brief_spike_multiwindow_did_not_page": transient["severity"] == "CLEAR",
                             "sustained_20_percent_error_page_detection_minutes": detection,
                             "simultaneously_firing_rules": 3, "severity_page_transitions": 1,
                             "missing_producer_status": held["status"], "missing_producer_quality": held["quality"],
                             "fast_multiwindow_reset_minutes": 5, "single_long_window_reset_minutes": 56,
                             "ticket_recovery_minutes": (end - outage_end) // 60,
                             "zero_traffic_status": no_traffic["status"], "batch_retry_replayed": replay["replayed"],
                             "rollup_integrity_passed": engine.verify("checkout")["passed"]},
                "delivery": {**delivered, "unique_inbox_events": len(events), "outbox_pending": len(engine.pending()),
                             "injected_crash_after_store_before_ack": True},
                "http_statuses": statuses, "benchmark": timings,
                "limits": ["scenario time and request-count aggregates are synthetic, not live user traffic",
                           "notification sink is a local SQLite inbox; no pager, SMS, email, or cloud service contacted",
                           "single local database; trusted producers, clock, and operator; loopback API only"]}
