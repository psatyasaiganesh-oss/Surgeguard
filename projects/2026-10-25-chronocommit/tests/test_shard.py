from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from chronocommit.errors import ConflictError, IntegrityError, NotFoundError, ValidationError
from chronocommit.model import Mutation, digest_json
from chronocommit.shard import MVCCShard


SECRET = b"test-shard-decision-secret-at-least-32"


class ShardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "shard.sqlite"
        self.shard = MVCCShard(self.path, "shard-00", SECRET)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def prepare(
        self,
        txn_id: str = "txn-1",
        *,
        commit_ts: int = 100,
        mutation: Mutation | None = None,
        digest: str | None = None,
    ) -> str:
        mutation = mutation or Mutation("key", {"value": 1}, 0)
        digest = digest or digest_json({"txn": txn_id})
        self.shard.prepare(
            txn_id=txn_id,
            tenant="tenant",
            commit_ts=commit_ts,
            payload_digest=digest,
            mutations=[mutation],
            authorization=self.shard.sign("PREPARE", txn_id, commit_ts, digest),
            now_ns=1,
        )
        return digest

    def commit(self, txn_id: str, commit_ts: int, digest: str) -> None:
        self.shard.commit(
            txn_id=txn_id,
            commit_ts=commit_ts,
            payload_digest=digest,
            authorization=self.shard.sign("COMMIT", txn_id, commit_ts, digest),
            now_ns=2,
        )

    def test_prepare_hides_uncommitted_intent(self) -> None:
        self.prepare()
        value = self.shard.latest("tenant", "key")
        self.assertTrue(value.deleted)
        self.assertEqual(value.commit_ts, 0)

    def test_commit_publishes_version(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        value = self.shard.latest("tenant", "key")
        self.assertEqual(value.value, {"value": 1})
        self.assertEqual(value.commit_ts, 100)

    def test_historical_read_uses_previous_version(self) -> None:
        first_digest = self.prepare()
        self.commit("txn-1", 100, first_digest)
        second_digest = self.prepare(
            "txn-2", commit_ts=200, mutation=Mutation("key", {"value": 2}, 100)
        )
        self.commit("txn-2", 200, second_digest)
        self.assertEqual(self.shard.read_at("tenant", "key", 150).value, {"value": 1})
        self.assertEqual(self.shard.read_at("tenant", "key", 250).value, {"value": 2})

    def test_delete_creates_tombstone(self) -> None:
        first_digest = self.prepare()
        self.commit("txn-1", 100, first_digest)
        second_digest = self.prepare(
            "txn-2", commit_ts=200, mutation=Mutation("key", None, 100, delete=True)
        )
        self.commit("txn-2", 200, second_digest)
        self.assertTrue(self.shard.latest("tenant", "key").deleted)

    def test_prepare_is_idempotent(self) -> None:
        digest = self.prepare()
        self.prepare(digest=digest)
        self.assertEqual(self.shard.transaction_state("txn-1"), "PREPARED")

    def test_commit_is_idempotent(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        self.commit("txn-1", 100, digest)
        self.assertEqual(self.shard.version_count(), 1)

    def test_prepare_replay_with_changed_timestamp_is_rejected(self) -> None:
        digest = self.prepare()
        with self.assertRaises(IntegrityError):
            self.prepare(commit_ts=101, digest=digest)

    def test_expected_version_conflict_is_rejected(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        with self.assertRaises(ConflictError):
            self.prepare("txn-2", commit_ts=200, mutation=Mutation("key", 2, 0))

    def test_second_intent_on_key_is_rejected(self) -> None:
        self.prepare()
        with self.assertRaises(ConflictError):
            self.prepare("txn-2", commit_ts=101, mutation=Mutation("key", 2, 0))

    def test_abort_removes_intent(self) -> None:
        self.prepare()
        self.shard.abort("txn-1", now_ns=2)
        self.assertEqual(self.shard.transaction_state("txn-1"), "ABORTED")
        self.prepare("txn-2", commit_ts=101)

    def test_abort_unknown_is_noop(self) -> None:
        self.shard.abort("txn-unknown")
        self.assertIsNone(self.shard.transaction_state("txn-unknown"))

    def test_committed_transaction_cannot_be_aborted(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        with self.assertRaises(IntegrityError):
            self.shard.abort("txn-1")

    def test_commit_requires_prepared_record(self) -> None:
        digest = digest_json({"missing": True})
        with self.assertRaises(NotFoundError):
            self.commit("txn-missing", 100, digest)

    def test_prepare_rejects_invalid_authorization(self) -> None:
        digest = digest_json({"txn": 1})
        with self.assertRaises(IntegrityError):
            self.shard.prepare(
                txn_id="txn-1",
                tenant="tenant",
                commit_ts=100,
                payload_digest=digest,
                mutations=[Mutation("key", 1, 0)],
                authorization="0" * 64,
            )

    def test_commit_rejects_invalid_authorization(self) -> None:
        digest = self.prepare()
        with self.assertRaises(IntegrityError):
            self.shard.commit(
                txn_id="txn-1",
                commit_ts=100,
                payload_digest=digest,
                authorization="0" * 64,
            )

    def test_tampered_intent_is_detected_before_commit(self) -> None:
        digest = self.prepare()
        connection = sqlite3.connect(self.path)
        connection.execute(
            "UPDATE intents SET value_json = ? WHERE txn_id = ?", ('{"value":999}', "txn-1")
        )
        connection.commit()
        connection.close()
        with self.assertRaises(IntegrityError):
            self.commit("txn-1", 100, digest)

    def test_tampered_version_is_detected_on_read(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE versions SET value_json = ?", ('{"value":999}',))
        connection.commit()
        connection.close()
        with self.assertRaises(IntegrityError):
            self.shard.latest("tenant", "key")

    def test_tampered_audit_chain_is_detected(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE audit_log SET detail_json = '{}' WHERE sequence = 1")
        connection.commit()
        connection.close()
        with self.assertRaises(IntegrityError):
            self.shard.verify_integrity()

    def test_wrong_secret_cannot_open_existing_shard(self) -> None:
        with self.assertRaises(IntegrityError):
            MVCCShard(self.path, "shard-00", b"different-secret-with-at-least-32-bytes")

    def test_short_secret_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            MVCCShard(Path(self.temp.name) / "bad.sqlite", "shard-01", b"short")

    def test_integrity_report_counts_versions_and_audits(self) -> None:
        digest = self.prepare()
        self.commit("txn-1", 100, digest)
        report = self.shard.verify_integrity()
        self.assertEqual(report["versions"], 1)
        self.assertEqual(report["intents"], 0)
        self.assertEqual(report["audit_entries"], 2)


if __name__ == "__main__":
    unittest.main()

