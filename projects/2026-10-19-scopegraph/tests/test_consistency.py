import concurrent.futures
import subprocess
import sys
import threading

from scopegraph.engine import Engine
from scopegraph.model import Problem
from .helpers import Fixture, create, delete, edge


class CacheTests(Fixture):
    def test_warm_hit_still_reads_latest_revision_from_other_engine(self):
        self.assertTrue(self.check()["allowed"])
        self.assertTrue(self.check()["cache_hit"])
        other = Engine(self.path, clock=self.clock)
        other.mutate("acme", [delete("group:backend", "member", "user:alice")], "other-revoke")
        result = self.check()
        self.assertFalse(result["allowed"])
        self.assertFalse(result["cache_hit"])
        self.assertEqual(2, result["revision"])

    def test_negative_cache_invalidated_by_new_grant(self):
        self.assertFalse(self.check("user:bob")["allowed"])
        self.assertTrue(self.check("user:bob")["cache_hit"])
        self.write([edge("document:plan", "viewer", "user:bob")])
        self.assertTrue(self.check("user:bob")["allowed"])

    def test_cache_is_bounded_lru(self):
        engine = Engine(self.path, clock=self.clock, cache_size=2)
        for user in ("user:a", "user:b", "user:c"):
            engine.check("acme", "document:plan", "view", user)
        self.assertEqual(2, engine.stats()["entries"])
        self.assertEqual(1, engine.stats()["evictions"])

    def test_cache_can_be_disabled(self):
        engine = Engine(self.path, clock=self.clock, cache_size=0)
        for _ in range(2):
            result = engine.check("acme", "document:plan", "view", "user:alice")
            self.assertFalse(result["cache_hit"])
        self.assertEqual(0, engine.stats()["entries"])

    def test_cache_ttl_is_enforced(self):
        self.check()
        self.clock.now += 61
        self.assertFalse(self.check()["cache_hit"])

    def test_observed_clock_rollback_clears_cache(self):
        self.write([edge("document:plan", "blocked", "user:alice", expires_at=self.clock() + 1)])
        self.clock.now += 2
        self.assertTrue(self.check()["allowed"])
        key = ("acme", 2, "document:plan", "view", "user:alice")
        future_entry = self.engine.cache[key]
        self.clock.now -= 2
        self.assertFalse(self.check()["allowed"])
        # Represent a concurrent future-time evaluation inserting after the clear.
        with self.engine.lock:
            self.engine.cache[key] = future_entry
        result = self.check()
        self.assertFalse(result["cache_hit"])
        self.assertFalse(result["allowed"])

    def test_revision_token_is_lower_bound_not_history_selector(self):
        token = self.seed["revision_token"]
        revoked = self.write([delete("group:backend", "member", "user:alice")])
        result = self.check(min_token=token)
        self.assertFalse(result["allowed"])
        self.assertEqual(revoked["revision"], result["revision"])

    def test_unavailable_revision_does_not_fall_back_to_allow(self):
        token = self.engine.tokens.revision("acme", 999)
        self.problem("revision_unavailable", self.check, min_token=token)

    def test_parallel_checks_after_committed_revoke_all_deny(self):
        self.check()
        self.write([delete("group:backend", "member", "user:alice")])
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.check(), range(32)))
        self.assertTrue(all(not result["allowed"] and result["revision"] == 2 for result in results))


class ContentTests(Fixture):
    def test_publish_and_protected_read(self):
        result = self.publish()
        read = self.engine.read_content("acme", "document:plan", "user:alice", result["revision_token"])
        self.assertEqual("version one", read["body"])
        self.assertEqual(1, read["version"])
        self.assertGreaterEqual(read["checked_revision"], read["content_acl_revision"])

    def test_permission_and_content_same_snapshot_during_revocation(self):
        self.publish()
        ready, release = threading.Event(), threading.Event()

        def pause():
            ready.set()
            self.assertTrue(release.wait(5))

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.engine.read_content, "acme", "document:plan", "user:alice",
                                 _after_authorize=pause)
            try:
                self.assertTrue(ready.wait(5))
                self.write([delete("group:backend", "member", "user:alice")])
                self.publish("new private version", "publish-two", 1)
            finally:
                release.set()
            observed = future.result(timeout=5)
        self.assertEqual("version one", observed["body"])
        self.assertEqual(1, observed["version"])
        self.problem("forbidden", self.engine.read_content, "acme", "document:plan", "user:alice")
        self.assertEqual("new private version", self.engine.read_content("acme", "document:plan", "user:owner")["body"])

    def test_revoked_writer_cannot_publish(self):
        self.write([delete("group:backend", "member", "user:alice")])
        self.problem("forbidden", self.engine.publish, "acme", "document:plan", "user:alice", "bad", "bad", 0)

    def test_content_compare_and_swap(self):
        self.publish()
        self.problem("conflict", self.publish, "stale", "publish-stale", 0)
        self.assertEqual("version one", self.engine.read_content("acme", "document:plan", "user:owner")["body"])

    def test_publish_replay_is_durable_and_does_not_increment_version(self):
        first = self.publish()
        other = Engine(self.path, clock=self.clock)
        replay = other.publish("acme", "document:plan", "user:owner", "version one", "publish-one", 0)
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["version"], replay["version"])
        self.assertEqual(1, other.read_content("acme", "document:plan", "user:owner")["version"])

    def test_publish_receipt_is_not_authorization_after_revocation(self):
        self.engine.publish("acme", "document:plan", "user:alice", "text", "alice-write", 0)
        self.write([delete("group:backend", "member", "user:alice")])
        self.problem("forbidden", self.engine.publish, "acme", "document:plan", "user:alice", "text", "alice-write", 0)

    def test_publish_request_id_conflict(self):
        self.publish()
        self.problem("conflict", self.publish, "different text", "publish-one", 0)

    def test_content_size_limit_counts_utf8_bytes(self):
        self.problem("invalid_input", self.publish, "\U0001f600" * 8193)
        self.publish("\U0001f600" * 8192)

    def test_missing_content_after_allow_is_404(self):
        self.problem("not_found", self.engine.read_content, "acme", "document:plan", "user:alice")

    def test_drop_recreate_does_not_restore_content(self):
        self.publish()
        self.write([{"op": "drop", "object": "document:plan"}, create("document:plan"),
                    edge("document:plan", "owner", "user:owner")])
        self.problem("not_found", self.engine.read_content, "acme", "document:plan", "user:owner")

    def test_parallel_content_cas_one_winner(self):
        def writer(index):
            try:
                self.publish(f"writer {index}", f"writer-{index}", 0)
                return "ok"
            except Problem as exc:
                return exc.code
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(writer, range(6)))
        self.assertEqual(1, results.count("ok"))
        self.assertEqual(5, results.count("conflict"))


class CrashTests(Fixture):
    def test_process_exit_rolls_back_uncommitted_acl_and_revision(self):
        code = """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute('BEGIN IMMEDIATE')
conn.execute("INSERT INTO tuples VALUES('acme','document:plan','viewer','user:crash',NULL)")
conn.execute("UPDATE tenants SET revision=99 WHERE id='acme'")
os._exit(23)
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path)], timeout=10, capture_output=True)
        self.assertEqual(23, result.returncode)
        reopened = Engine(self.path, clock=self.clock)
        check = reopened.check("acme", "document:plan", "view", "user:crash")
        self.assertFalse(check["allowed"])
        self.assertEqual(1, check["revision"])

    def test_committed_write_and_receipt_survive_abrupt_exit(self):
        code = """
import os, sys
from scopegraph.engine import Engine
Engine(sys.argv[1]).mutate('acme', [{'op':'touch','resource':'document:plan',
 'relation':'viewer','subject':'user:durable'}], 'durable')
os._exit(24)
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path)], timeout=10, capture_output=True)
        self.assertEqual(24, result.returncode)
        reopened = Engine(self.path, clock=self.clock)
        self.assertTrue(reopened.check("acme", "document:plan", "view", "user:durable")["allowed"])
        self.assertTrue(reopened.mutate("acme", [edge("document:plan", "viewer", "user:durable")], "durable")["replayed"])
