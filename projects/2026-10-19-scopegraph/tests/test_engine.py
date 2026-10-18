import concurrent.futures
import random
import sqlite3

from scopegraph.engine import Engine
from .helpers import Fixture, create, delete, edge


class PolicyTests(Fixture):
    def test_nested_group_inheritance_has_proof(self):
        result = self.check()
        self.assertTrue(result["allowed"])
        self.assertEqual(4, len(result["proof"]))
        self.assertIn("group:backend#member@user:alice", result["proof"])

    def test_owner_inherits_all_permissions(self):
        for permission in ("view", "edit", "share"):
            self.assertTrue(self.check("user:owner", permission)["allowed"])

    def test_editor_cannot_share(self):
        self.assertTrue(self.check(perm="edit")["allowed"])
        self.assertFalse(self.check(perm="share")["allowed"])

    def test_viewer_cannot_edit(self):
        self.write([edge("document:plan", "viewer", "user:bob")])
        self.assertTrue(self.check("user:bob")["allowed"])
        self.assertFalse(self.check("user:bob", "edit")["allowed"])

    def test_default_deny(self):
        self.assertEqual("no_grant", self.check("user:nobody")["reason"])

    def test_unknown_object_denied(self):
        self.assertEqual("not_found", self.check(resource="document:missing")["reason"])

    def test_groups_are_not_content_resources(self):
        self.assertFalse(self.check(resource="group:eng")["allowed"])

    def test_ancestor_block_overrides_direct_owner(self):
        self.write([edge("document:plan", "owner", "user:alice"),
                    edge("folder:root", "blocked", "user:alice")])
        result = self.check()
        self.assertFalse(result["allowed"])
        self.assertEqual("explicit_deny", result["reason"])

    def test_nested_group_block_overrides_owner(self):
        self.write([edge("document:plan", "owner", "user:alice"),
                    edge("folder:root", "blocked", "group:eng#member")])
        self.assertEqual("explicit_deny", self.check()["reason"])

    def test_unrelated_block_does_not_deny(self):
        self.write([edge("document:plan", "blocked", "user:bob")])
        self.assertTrue(self.check()["allowed"])

    def test_expiring_group_membership(self):
        self.write([edge("group:backend", "member", "user:alice", expires_at=self.clock() + 10)])
        self.assertTrue(self.check()["allowed"])
        self.clock.now += 10
        self.assertFalse(self.check()["allowed"])

    def test_expiring_block_invalidates_negative_cache(self):
        self.write([edge("document:plan", "blocked", "user:alice", expires_at=self.clock() + 10)])
        self.assertFalse(self.check()["allowed"])
        self.assertTrue(self.check()["cache_hit"])
        self.clock.now += 10
        self.assertTrue(self.check()["allowed"])

    def test_expiring_parent_stops_inheritance(self):
        self.write([edge("document:plan", "parent", "folder:team", expires_at=self.clock() + 1)])
        self.assertTrue(self.check()["allowed"])
        self.clock.now += 1
        self.assertFalse(self.check()["allowed"])

    def test_atomic_folder_move(self):
        self.write([delete("document:plan", "parent", "folder:team"),
                    edge("document:plan", "parent", "folder:other")])
        self.assertFalse(self.check()["allowed"])

    def test_edge_budget_cannot_allow_before_checking_denies(self):
        engine = Engine(self.path, clock=self.clock, max_edges=1)
        result = engine.check("acme", "document:plan", "view", "user:owner")
        self.assertFalse(result["allowed"])
        self.assertEqual("budget_exceeded", result["reason"])

    def test_depth_budget_fails_closed(self):
        engine = Engine(self.path, clock=self.clock, max_depth=1)
        self.assertEqual("budget_exceeded", engine.check("acme", "document:plan", "view", "user:owner")["reason"])

    def test_random_dag_matches_independent_reachability_oracle(self):
        rng = random.Random(713)
        groups = [f"group:g{i}" for i in range(18)]
        operations = [create(ref) for ref in groups] + [create("document:random")]
        graph, users = {ref: [] for ref in groups}, {ref: set() for ref in groups}
        for i, ref in enumerate(groups):
            for next_ref in groups[i + 1:]:
                if rng.random() < .16:
                    graph[ref].append(next_ref)
                    operations.append(edge(ref, "member", next_ref + "#member"))
            for user in range(12):
                if rng.random() < .08:
                    users[ref].add(f"user:u{user}")
                    operations.append(edge(ref, "member", f"user:u{user}"))
        operations.append(edge("document:random", "viewer", groups[0] + "#member"))
        self.write(operations)
        expected = set()
        todo, seen = [groups[0]], set()
        while todo:
            ref = todo.pop()
            if ref not in seen:
                seen.add(ref)
                expected.update(users[ref])
                todo.extend(graph[ref])
        for user in range(12):
            name = f"user:u{user}"
            self.assertEqual(name in expected, self.check(name, resource="document:random")["allowed"])


class MutationTests(Fixture):
    def test_cycle_rejects_entire_batch_and_receipt(self):
        revision = self.check()["revision"]
        self.problem("cycle", self.engine.mutate, "acme", [edge("document:plan", "viewer", "user:mallory"),
                     edge("group:backend", "member", "group:eng#member")], "cycle")
        self.assertFalse(self.check("user:mallory")["allowed"])
        self.assertEqual(revision, self.check()["revision"])
        self.engine.mutate("acme", [edge("document:plan", "viewer", "user:mallory")], "cycle")
        self.assertTrue(self.check("user:mallory")["allowed"])

    def test_folder_cycle_rejected(self):
        self.problem("cycle", self.write, [edge("folder:root", "parent", "folder:team")])

    def test_self_group_cycle_rejected_even_expired(self):
        self.problem("cycle", self.write, [edge("group:eng", "member", "group:eng#member", expires_at=1)])

    def test_multiple_parents_rejected(self):
        self.problem("conflict", self.write, [edge("document:plan", "parent", "folder:other")])

    def test_missing_reference_rolls_back_creates(self):
        self.problem("not_found", self.write, [create("document:new"),
                     edge("document:new", "viewer", "group:missing#member")])
        self.assertEqual("not_found", self.check(resource="document:new")["reason"])

    def test_drop_group_removes_incoming_and_outgoing_grants(self):
        self.write([{"op": "drop", "object": "group:backend"}, create("group:backend"),
                    edge("group:backend", "member", "user:alice")])
        self.assertFalse(self.check()["allowed"])

    def test_drop_folder_removes_incoming_parent_links(self):
        self.write([{"op": "drop", "object": "folder:team"}, create("folder:team"),
                    edge("folder:team", "owner", "user:alice")])
        self.assertFalse(self.check()["allowed"])

    def test_mutation_idempotency_survives_new_engine(self):
        ops = [edge("document:plan", "viewer", "user:bob")]
        first = self.engine.mutate("acme", ops, "once", expected_revision=1)
        other = Engine(self.path, clock=self.clock)
        self.assertEqual(2, other.create_tenant("acme")["revision"])
        again = other.mutate("acme", ops, "once", expected_revision=1)
        self.assertEqual(first["revision"], again["revision"])
        self.assertEqual(first["revision_token"], again["revision_token"])
        self.assertTrue(again["replayed"])

    def test_request_id_conflicting_payload(self):
        self.engine.mutate("acme", [create("document:new")], "once")
        self.problem("conflict", self.engine.mutate, "acme", [create("document:different")], "once")

    def test_revision_compare_and_swap(self):
        self.write([create("document:new")], expected_revision=1)
        self.problem("conflict", self.write, [create("document:stale")], expected_revision=1)

    def test_parallel_compare_and_swap_has_one_winner(self):
        def writer(index):
            try:
                self.engine.mutate("acme", [create(f"document:p{index}")], f"parallel-{index}", 1)
                return "ok"
            except Exception as exc:
                return getattr(exc, "code", type(exc).__name__)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(writer, range(8)))
        self.assertEqual(1, results.count("ok"))
        self.assertEqual(7, results.count("conflict"))

    def test_concurrent_idempotent_retries_increment_once(self):
        def retry(_):
            return self.engine.mutate("acme", [create("document:once")], "retry")["revision"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(retry, range(8)))
        self.assertEqual([2] * 8, results)

    def test_tenant_isolation_same_object_and_user_ids(self):
        self.engine.create_tenant("beta")
        self.engine.mutate("beta", [create("document:plan")], "seed")
        self.assertFalse(self.engine.check("beta", "document:plan", "view", "user:owner")["allowed"])
        self.assertTrue(self.check("user:owner")["allowed"])

    def test_sql_like_identifier_rejected_without_modification(self):
        self.problem("invalid_input", self.write, [create("document:x';DROP_TABLE_tenants;--")])
        self.assertTrue(self.check()["allowed"])

    def test_audit_only_contains_committed_revisions(self):
        self.write([create("document:new")])
        self.problem("cycle", self.write, [edge("group:eng", "member", "group:eng#member")])
        with sqlite3.connect(self.path) as conn:
            self.assertEqual([(1, 13), (2, 1)], conn.execute(
                "SELECT revision,operations FROM audit ORDER BY revision").fetchall())
