"""Synthetic, credential-free scenarios exercising the real engine and HTTP API."""

import concurrent.futures
import http.client
import json
import platform
import sqlite3
import statistics
import tempfile
import threading
import time
from pathlib import Path

from .engine import Engine
from .http import Server
from .model import Problem


class Clock:
    def __init__(self, now=2000000000):
        self.now = now

    def __call__(self):
        return self.now


def create(ref):
    return {"op": "create", "object": ref}


def edge(resource, relation, subject, *, delete=False, expires_at=None):
    result = {"op": "delete" if delete else "touch", "resource": resource,
              "relation": relation, "subject": subject}
    if expires_at is not None:
        result["expires_at"] = expires_at
    return result


def expect_problem(code, action):
    try:
        action()
    except Problem as exc:
        assert exc.code == code, (exc.code, code)
        return True
    raise AssertionError("expected failure: " + code)


def seed(engine):
    engine.create_tenant("acme")
    result = engine.mutate("acme", [
        create("folder:team"), create("group:eng"), create("group:backend"), create("document:plan"),
        edge("group:eng", "member", "group:backend#member"),
        edge("group:backend", "member", "user:alice"),
        edge("folder:team", "editor", "group:eng#member"),
        edge("folder:team", "owner", "user:owner"),
        edge("document:plan", "parent", "folder:team"),
    ], "seed")
    engine.publish("acme", "document:plan", "user:owner", "synthetic version one", "publish-one", 0)
    return result


def snapshot_race(engine):
    engine.mutate("acme", [edge("document:plan", "viewer", "user:carl")], "race-grant")
    before = engine.read_content("acme", "document:plan", "user:carl")
    ready, release = threading.Event(), threading.Event()

    def barrier():
        ready.set()
        assert release.wait(10), "race test barrier timed out"

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(engine.read_content, "acme", "document:plan", "user:carl",
                             _after_authorize=barrier)
        try:
            assert ready.wait(10), "reader did not reach authorization barrier"
            revoked = engine.mutate("acme", [edge("document:plan", "viewer", "user:carl", delete=True)],
                                    "race-revoke")
            written = engine.publish("acme", "document:plan", "user:owner", "synthetic NEW secret",
                                     "race-publish", before["version"], revoked["revision_token"])
        finally:
            release.set()
        old_read = future.result(timeout=10)
    assert old_read["body"] == before["body"]
    assert old_read["version"] < written["version"]
    denied = expect_problem("forbidden", lambda: engine.read_content("acme", "document:plan", "user:carl"))
    return {"old_snapshot_version": old_read["version"], "new_content_version": written["version"],
            "inflight_reader_never_observed_new_content": True, "fresh_reader_denied": denied}


def http_scenarios(engine):
    statuses = {}
    with Server(engine) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            alice = engine.mint("acme", "user:alice")
            owner = engine.mint("acme", "user:owner")
            cases = [
                ("revoked_read", alice, "/v1/read", {"resource": "document:plan"}, 403),
                ("owner_check", owner, "/v1/check", {"resource": "document:plan", "permission": "view"}, 200),
                ("identity_override", alice, "/v1/check", {"resource": "document:plan", "permission": "view",
                                                         "subject": "user:owner"}, 400),
                ("non_admin_write", alice, "/v1/relationships", {"operations": [create("document:evil")],
                                                                "request_id": "evil"}, 403),
            ]
            for name, token, route, body, expected in cases:
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    conn.request("POST", route, json.dumps(body),
                                 {"Content-Type": "application/json", "Authorization": "Bearer " + token})
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


def benchmark(path, count=1000):
    engine = Engine(path, clock=Clock(), cache_size=2048)
    engine.create_tenant("bench")
    engine.mutate("bench", [create("folder:root"), edge("folder:root", "owner", "user:reader")], "root")
    operations = []
    for index in range(count):
        ref = f"document:d{index}"
        operations += [create(ref), edge(ref, "parent", "folder:root")]
    for offset in range(0, len(operations), 200):
        engine.mutate("bench", operations[offset:offset + 200], f"batch-{offset}")

    def measure():
        latencies = []
        started = time.perf_counter()
        for index in range(count):
            start = time.perf_counter()
            result = engine.check("bench", f"document:d{index}", "view", "user:reader")
            latencies.append((time.perf_counter() - start) * 1000)
            assert result["allowed"]
        elapsed = time.perf_counter() - started
        ordered = sorted(latencies)
        return {"checks": count, "seconds": round(elapsed, 6),
                "checks_per_second": round(count / elapsed, 2),
                "p50_ms": round(statistics.median(latencies), 4),
                "p95_ms": round(ordered[int((len(ordered) - 1) * .95)], 4)}

    cold, warm = measure(), measure()
    stats = engine.stats()
    assert stats["misses"] == count and stats["hits"] == count
    return {"objects": count + 1, "tuples": count + 1, "cold": cold, "warm": warm,
            "cache": stats, "clock": "fixed synthetic clock; no expiries during benchmark",
            "scope": "single process, sequential checks, SQLite WAL on temporary local disk"}


def run():
    with tempfile.TemporaryDirectory(prefix="scopegraph-demo-") as folder:
        clock = Clock()
        engine = Engine(Path(folder) / "demo.db", clock=clock)
        original = seed(engine)
        first = engine.check("acme", "document:plan", "view", "user:alice")
        assert first["allowed"] and len(first["proof"]) == 4
        assert engine.check("acme", "document:plan", "view", "user:alice")["cache_hit"]
        revoked = engine.mutate("acme", [edge("group:backend", "member", "user:alice", delete=True)], "revoke")
        engine.publish("acme", "document:plan", "user:owner", "synthetic private version two", "publish-two", 1,
                       revoked["revision_token"])
        old_token_denied = expect_problem("forbidden", lambda: engine.read_content(
            "acme", "document:plan", "user:alice", original["revision_token"]))
        new_token_denied = expect_problem("forbidden", lambda: engine.read_content(
            "acme", "document:plan", "user:alice", revoked["revision_token"]))
        naive_stale_allow = first["allowed"]  # Deliberately isolated TTL-only counterexample.
        assert not engine.check("acme", "document:plan", "view", "user:alice")["allowed"]
        expiring = engine.mutate("acme", [edge("document:plan", "viewer", "user:bob", expires_at=clock() + 10)],
                                 "temporary-grant")
        assert engine.check("acme", "document:plan", "view", "user:bob")["allowed"]
        clock.now += 11
        expired = engine.check("acme", "document:plan", "view", "user:bob")
        assert not expired["allowed"] and expired["revision"] == expiring["revision"]
        rolled_back = expect_problem("cycle", lambda: engine.mutate("acme", [
            edge("document:plan", "viewer", "user:mallory"),
            edge("group:backend", "member", "group:eng#member"),
        ], "bad-cycle"))
        after = engine.check("acme", "document:plan", "view", "user:mallory")
        assert not after["allowed"] and after["revision"] == expiring["revision"]
        engine.create_tenant("beta")
        engine.mutate("beta", [create("document:plan")], "beta-seed")
        cross_tenant_denied = not engine.check("beta", "document:plan", "view", "user:owner")["allowed"]
        token_isolated = expect_problem("invalid_token", lambda: engine.check(
            "beta", "document:plan", "view", "user:owner", revoked["revision_token"]))
        race = snapshot_race(engine)
        statuses = http_scenarios(engine)
        timings = benchmark(Path(folder) / "benchmark.db")
        return {"project": "ScopeGraph", "implementation": "independent Google Zanzibar-inspired local lab",
                "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                                "system": platform.system()},
                "scenarios": {"nested_group_inheritance": first["allowed"],
                              "proof_edges": len(first["proof"]),
                              "ttl_only_counterexample_would_allow_after_revoke": naive_stale_allow,
                              "old_revision_token_does_not_restore_access": old_token_denied,
                              "revocation_token_read_denied": new_token_denied,
                              "expiry_without_revision_change_denied": not expired["allowed"],
                              "cycle_batch_fully_rolled_back": rolled_back and not after["allowed"],
                              "tenant_access_isolated": cross_tenant_denied,
                              "tenant_revision_token_isolated": token_isolated},
                "concurrent_read": race, "http_statuses": statuses, "benchmark": timings,
                "limits": ["one local SQLite database; not a distributed Zanzibar implementation",
                           "synthetic microbenchmark; not a production SLA or company performance result",
                           "trusted operator and trusted server clock; loopback API only"]}
