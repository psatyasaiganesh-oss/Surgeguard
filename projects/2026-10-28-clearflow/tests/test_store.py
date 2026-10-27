from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from clearflow.errors import (
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    NotFoundError,
    StateError,
    ValidationError,
)
from clearflow.store import ClearFlowStore


SECRET = b"clearflow-unit-test-secret-at-least-32-bytes"
BASE = 1_000_000_000_000
SECOND = 1_000_000_000


def policy(**overrides):
    result = {
        "max_lag_ns": 10 * SECOND,
        "clearing_grace_ns": 10 * SECOND,
        "max_missing_references": 0,
        "minimum_score": 100.0,
    }
    result.update(overrides)
    return result


def money_event(
    event_id="evt:create",
    reference="flow:create",
    pending="pending:1",
    *,
    release=False,
    currency="USD",
    occurred=BASE + SECOND,
):
    postings = (
        [(pending, -100), ("merchant", 97), ("fees", 3)]
        if release
        else [("cash", -100), (pending, 100)]
    )
    return {
        "event_id": event_id,
        "producer": "payments",
        "reference_id": reference,
        "currency": currency,
        "occurred_ns": occurred,
        "postings": [
            {"account_id": account_id, "amount_minor": amount}
            for account_id, amount in postings
        ],
        "metadata": {},
    }


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "clearflow.sqlite"
        self.store = ClearFlowStore(self.path, secret=SECRET)
        self.tenant = "shop"
        for account, kind in (
            ("cash", "terminal"),
            ("merchant", "terminal"),
            ("fees", "terminal"),
            ("pending:1", "clearing"),
            ("pending:wrong", "clearing"),
        ):
            self.store.create_account(self.tenant, account, kind, "USD", now_ns=BASE)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def ingest_create(self, *, tenant=None, key="key:create", event_id="evt:create"):
        return self.store.ingest_event(
            tenant or self.tenant,
            key,
            money_event(event_id=event_id),
            received_ns=BASE + 2 * SECOND,
        )

    def ingest_release(self, *, pending="pending:1", received=BASE + 3 * SECOND):
        return self.store.ingest_event(
            self.tenant,
            "key:release",
            money_event(
                event_id="evt:release",
                reference="flow:release",
                pending=pending,
                release=True,
                occurred=BASE + 2 * SECOND,
            ),
            received_ns=received,
        )

    def manifest(self):
        return self.store.publish_manifest(
            self.tenant,
            "key:manifest",
            "batch:1",
            "payments",
            ["flow:create", "flow:release"],
            BASE + 10 * SECOND,
            now_ns=BASE,
        )

    def evaluate(self, key="key:quality", **policy_overrides):
        return self.store.evaluate_quality(
            self.tenant,
            key,
            BASE + 30 * SECOND,
            policy(**policy_overrides),
            now_ns=BASE + 30 * SECOND,
        )

    def test_short_secret_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            ClearFlowStore(Path(self.temporary.name) / "bad.sqlite", secret=b"short")

    def test_existing_database_rejects_other_secret(self) -> None:
        with self.assertRaises(IntegrityError):
            ClearFlowStore(
                self.path, secret=b"a-different-secret-that-is-definitely-long-enough"
            )

    def test_account_creation_is_idempotent(self) -> None:
        result = self.store.create_account(self.tenant, "cash", "terminal", "USD")
        self.assertTrue(result["replayed"])

    def test_account_definition_cannot_change(self) -> None:
        with self.assertRaises(ConflictError):
            self.store.create_account(self.tenant, "cash", "clearing", "USD")

    def test_ingest_updates_balances(self) -> None:
        self.ingest_create()
        balances = {
            row["account_id"]: row["balance_minor"]
            for row in self.store.account_balances(self.tenant)["balances"]
        }
        self.assertEqual(balances["pending:1"], 100)
        self.assertEqual(balances["cash"], -100)

    def test_unknown_account_is_rejected(self) -> None:
        body = money_event()
        body["postings"][1]["account_id"] = "missing"
        with self.assertRaises(ValidationError):
            self.store.ingest_event(self.tenant, "unknown", body, received_ns=BASE + SECOND * 2)

    def test_account_currency_mismatch_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.ingest_event(
                self.tenant,
                "wrong-currency",
                money_event(currency="EUR"),
                received_ns=BASE + SECOND * 2,
            )

    def test_received_before_occurred_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.ingest_event(
                self.tenant, "future", money_event(), received_ns=BASE
            )

    def test_event_ingest_is_idempotent(self) -> None:
        first = self.ingest_create()
        replay = self.ingest_create()
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["event_hash"], replay["event_hash"])

    def test_changed_idempotent_event_is_rejected(self) -> None:
        self.ingest_create()
        changed = money_event()
        changed["metadata"] = {"changed": "yes"}
        with self.assertRaises(IdempotencyConflict):
            self.store.ingest_event(
                self.tenant, "key:create", changed, received_ns=BASE + 2 * SECOND
            )

    def test_duplicate_event_id_with_new_key_is_rejected(self) -> None:
        self.ingest_create()
        with self.assertRaises(ConflictError):
            self.ingest_create(key="new-key")

    def test_concurrent_retries_commit_one_event(self) -> None:
        def ingest(_: int):
            return self.ingest_create()

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(ingest, range(8)))
        self.assertEqual(sum(not item["replayed"] for item in results), 1)
        self.assertEqual(self.store.stats(self.tenant)["events"], 1)

    def test_out_of_order_events_clear(self) -> None:
        self.ingest_release()
        self.ingest_create()
        balances = self.store.account_balances(self.tenant)["balances"]
        pending = next(row for row in balances if row["account_id"] == "pending:1")
        self.assertEqual(pending["balance_minor"], 0)

    def test_manifest_is_idempotent(self) -> None:
        first = self.manifest()
        replay = self.manifest()
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])

    def test_changed_manifest_retry_is_rejected(self) -> None:
        self.manifest()
        with self.assertRaises(IdempotencyConflict):
            self.store.publish_manifest(
                self.tenant,
                "key:manifest",
                "batch:1",
                "payments",
                ["different"],
                BASE + 10 * SECOND,
            )

    def test_duplicate_batch_with_new_key_is_rejected(self) -> None:
        self.manifest()
        with self.assertRaises(ConflictError):
            self.store.publish_manifest(
                self.tenant,
                "new-key",
                "batch:1",
                "payments",
                ["flow:create", "flow:release"],
                BASE + 10 * SECOND,
            )

    def test_quality_passes_complete_cleared_flow(self) -> None:
        self.manifest()
        self.ingest_create()
        self.ingest_release()
        self.assertEqual(self.evaluate()["report"]["verdict"], "PASS")

    def test_quality_fails_missing_reference(self) -> None:
        self.manifest()
        self.ingest_create()
        result = self.evaluate()["report"]
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["coverage"]["missing_references"], 1)

    def test_quality_fails_uncleared_accounts(self) -> None:
        self.manifest()
        self.ingest_create()
        self.ingest_release(pending="pending:wrong")
        result = self.evaluate()["report"]
        self.assertEqual(result["coverage"]["uncleared_accounts"], 2)
        self.assertIn("uncleared_balances", result["reasons"])

    def test_quality_fails_late_event(self) -> None:
        self.manifest()
        self.ingest_create()
        self.ingest_release(received=BASE + 20 * SECOND)
        result = self.evaluate()["report"]
        self.assertEqual(result["coverage"]["late_events"], 1)
        self.assertIn("late_events", result["reasons"])

    def test_quality_is_inconclusive_without_manifest(self) -> None:
        self.ingest_create()
        self.ingest_release()
        result = self.evaluate()["report"]
        self.assertEqual(result["verdict"], "INCONCLUSIVE")
        self.assertIn("no_due_manifests", result["reasons"])

    def test_evaluation_is_idempotent(self) -> None:
        self.manifest()
        self.ingest_create()
        self.ingest_release()
        first = self.evaluate()
        replay = self.evaluate()
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["attestation"], replay["attestation"])

    def test_changed_evaluation_retry_is_rejected(self) -> None:
        self.evaluate()
        with self.assertRaises(IdempotencyConflict):
            self.evaluate(max_lag_ns=99)

    def test_attestation_verifies(self) -> None:
        result = self.evaluate()
        self.assertTrue(
            self.store.verify_attestation(
                result["attestation"], expected_tenant=self.tenant
            )["valid"]
        )

    def test_attestation_is_tenant_bound(self) -> None:
        result = self.evaluate()
        with self.assertRaises(NotFoundError):
            self.store.verify_attestation(result["attestation"], expected_tenant="other")

    def test_tampered_attestation_is_rejected(self) -> None:
        result = self.evaluate()
        result["attestation"]["payload"]["verdict"] = "PASS"
        with self.assertRaises(IntegrityError):
            self.store.verify_attestation(result["attestation"])

    def proposal(self):
        self.ingest_create()
        release = self.ingest_release(pending="pending:wrong")
        replacement = money_event(
            event_id="evt:release:fixed",
            reference="flow:release",
            pending="pending:1",
            release=True,
            occurred=BASE + 2 * SECOND,
        )
        return self.store.propose_correction(
            self.tenant,
            "key:proposal",
            "author",
            release["event_id"],
            replacement,
            now_ns=BASE + 4 * SECOND,
        )

    def test_proposal_contains_balance_impact(self) -> None:
        proposal = self.proposal()
        self.assertEqual(proposal["state"], "PROPOSED")
        self.assertEqual(len(proposal["preview"]["affected_accounts"]), 4)

    def test_proposal_unknown_target_is_rejected(self) -> None:
        with self.assertRaises(NotFoundError):
            self.store.propose_correction(
                self.tenant,
                "proposal",
                "author",
                "missing",
                money_event(event_id="replacement"),
            )

    def test_proposal_currency_must_match_target(self) -> None:
        self.ingest_create()
        with self.assertRaises(ValidationError):
            self.store.propose_correction(
                self.tenant,
                "proposal",
                "author",
                "evt:create",
                money_event(event_id="replacement", currency="EUR"),
            )

    def test_creator_cannot_approve(self) -> None:
        proposal = self.proposal()
        with self.assertRaises(ValidationError):
            self.store.approve_correction(
                self.tenant,
                proposal["proposal_id"],
                "author",
                proposal["preview_digest"],
            )

    def test_approval_must_bind_preview(self) -> None:
        proposal = self.proposal()
        with self.assertRaises(ConflictError):
            self.store.approve_correction(
                self.tenant, proposal["proposal_id"], "reviewer", "0" * 64
            )

    def test_two_distinct_approvals_change_state(self) -> None:
        proposal = self.proposal()
        first = self.store.approve_correction(
            self.tenant, proposal["proposal_id"], "reviewer-a", proposal["preview_digest"]
        )
        second = self.store.approve_correction(
            self.tenant, proposal["proposal_id"], "reviewer-b", proposal["preview_digest"]
        )
        self.assertEqual(first["state"], "PROPOSED")
        self.assertEqual(second["state"], "APPROVED")

    def test_execution_before_approvals_is_rejected(self) -> None:
        proposal = self.proposal()
        with self.assertRaises(StateError):
            self.store.execute_correction(
                self.tenant,
                proposal["proposal_id"],
                "operator",
                "execute",
                proposal["base_revision"],
            )

    def approve(self, proposal):
        for reviewer in ("reviewer-a", "reviewer-b"):
            self.store.approve_correction(
                self.tenant, proposal["proposal_id"], reviewer, proposal["preview_digest"]
            )

    def test_stale_preview_cannot_execute(self) -> None:
        proposal = self.proposal()
        self.approve(proposal)
        self.store.ingest_event(
            self.tenant,
            "unrelated",
            money_event(event_id="evt:other", reference="other"),
            received_ns=BASE + 5 * SECOND,
        )
        with self.assertRaises(ConflictError):
            self.store.execute_correction(
                self.tenant,
                proposal["proposal_id"],
                "operator",
                "execute",
                proposal["base_revision"],
            )

    def test_correction_appends_reversal_and_replacement(self) -> None:
        proposal = self.proposal()
        self.approve(proposal)
        result = self.store.execute_correction(
            self.tenant,
            proposal["proposal_id"],
            "operator",
            "execute",
            proposal["base_revision"],
            now_ns=BASE + 5 * SECOND,
        )
        self.assertEqual(result["revision"], proposal["base_revision"] + 2)
        self.assertEqual(self.store.stats(self.tenant)["events"], 4)

    def test_correction_execution_is_idempotent(self) -> None:
        proposal = self.proposal()
        self.approve(proposal)
        first = self.store.execute_correction(
            self.tenant, proposal["proposal_id"], "operator", "execute", proposal["base_revision"]
        )
        replay = self.store.execute_correction(
            self.tenant, proposal["proposal_id"], "operator", "execute", proposal["base_revision"]
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["revision"], replay["revision"])

    def test_applied_correction_rejects_other_execution_key(self) -> None:
        proposal = self.proposal()
        self.approve(proposal)
        self.store.execute_correction(
            self.tenant, proposal["proposal_id"], "operator", "execute", proposal["base_revision"]
        )
        with self.assertRaises(IdempotencyConflict):
            self.store.execute_correction(
                self.tenant, proposal["proposal_id"], "operator", "other", proposal["base_revision"]
            )

    def test_correction_restores_clearing_balances(self) -> None:
        proposal = self.proposal()
        self.approve(proposal)
        self.store.execute_correction(
            self.tenant,
            proposal["proposal_id"],
            "operator",
            "execute",
            proposal["base_revision"],
            now_ns=BASE + 5 * SECOND,
        )
        balances = {
            row["account_id"]: row["balance_minor"]
            for row in self.store.account_balances(self.tenant, as_of_ns=BASE + 10 * SECOND)[
                "balances"
            ]
        }
        self.assertEqual(balances["pending:1"], 0)
        self.assertEqual(balances["pending:wrong"], 0)

    def test_event_rows_are_immutable_in_sqlite(self) -> None:
        self.ingest_create()
        with sqlite3.connect(self.path) as connection, self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE events SET reference_id = 'changed' WHERE event_id = 'evt:create'"
            )

    def test_tenant_cannot_read_other_tenant_event(self) -> None:
        self.ingest_create()
        with self.assertRaises(NotFoundError):
            self.store.get_event("other", "evt:create")

    def test_integrity_verifies_complete_state(self) -> None:
        self.manifest()
        self.ingest_create()
        self.ingest_release()
        self.evaluate()
        result = self.store.verify_integrity(self.tenant)
        self.assertTrue(result["valid"])
        self.assertEqual(result["events"], 2)

    def test_integrity_detects_quality_report_tampering(self) -> None:
        result = self.evaluate()
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "UPDATE quality_runs SET report_json = ? WHERE evaluation_id = ?",
                (json.dumps({"verdict": "PASS"}), result["evaluation_id"]),
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity(self.tenant)

    def test_integrity_detects_audit_tampering(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "UPDATE audit_log SET action = 'ALTERED' WHERE tenant = ? AND sequence = 1",
                (self.tenant,),
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity(self.tenant)

    def test_stats_report_materialized_counts(self) -> None:
        self.ingest_create()
        stats = self.store.stats(self.tenant)
        self.assertEqual(stats["events"], 1)
        self.assertEqual(stats["postings"], 2)
        self.assertEqual(stats["accounts"], 5)


if __name__ == "__main__":
    unittest.main()
