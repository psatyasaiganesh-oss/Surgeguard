from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from mergemesh.model import ConflictError, IntegrityError, Operation, ValidationError
from mergemesh.store import RegionStore


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.east = RegionStore(self.root / "east.db", "east")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def seed_two_regions(self) -> tuple[RegionStore, RegionStore]:
        west = RegionStore(self.root / "west.db", "west")
        result = self.east.patch("acme", "sku", changes={"price": 100, "status": "ok"})
        west.apply_remote(self.east.operation(result["op_id"]))
        return self.east, west


class BasicStoreTests(StoreTestCase):
    def test_empty_document_has_stable_etag(self) -> None:
        first = self.east.get_document("acme", "missing")
        second = self.east.get_document("acme", "missing")
        self.assertEqual(first["etag"], second["etag"])
        self.assertEqual(first["values"], {})

    def test_patch_materializes_fields(self) -> None:
        result = self.east.patch("acme", "sku", changes={"price": 9, "active": True})
        self.assertEqual(result["op_id"], "east:1")
        self.assertEqual(result["document"]["values"], {"active": True, "price": 9})

    def test_patch_delete_creates_tombstone(self) -> None:
        self.east.patch("acme", "sku", changes={"note": "x"})
        self.east.patch("acme", "sku", delete_fields=["note"])
        document = self.east.get_document("acme", "sku")
        self.assertIn("note", document["deleted_fields"])
        self.assertNotIn("note", document["values"])

    def test_null_is_a_value_not_a_delete(self) -> None:
        self.east.patch("acme", "sku", changes={"optional": None})
        document = self.east.get_document("acme", "sku")
        self.assertIn("optional", document["values"])
        self.assertIsNone(document["values"]["optional"])

    def test_etag_precondition_accepts_current(self) -> None:
        current = self.east.get_document("acme", "sku")
        result = self.east.patch(
            "acme", "sku", changes={"price": 1}, expected_etag=current["etag"]
        )
        self.assertFalse(result["duplicate"])

    def test_etag_precondition_rejects_stale(self) -> None:
        stale = self.east.get_document("acme", "sku")["etag"]
        self.east.patch("acme", "sku", changes={"price": 1})
        with self.assertRaisesRegex(ConflictError, "etag"):
            self.east.patch("acme", "sku", changes={"price": 2}, expected_etag=stale)

    def test_invalid_etag_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.east.patch("acme", "sku", changes={"x": 1}, expected_etag="not-a-hash")

    def test_state_survives_reopen(self) -> None:
        self.east.patch("acme", "sku", changes={"price": 1})
        reopened = RegionStore(self.root / "east.db", "east")
        self.assertEqual(reopened.get_document("acme", "sku")["values"]["price"], 1)

    def test_region_identity_is_fenced(self) -> None:
        with self.assertRaisesRegex(IntegrityError, "belongs"):
            RegionStore(self.root / "east.db", "west")

    def test_local_sequence_is_monotonic_across_documents(self) -> None:
        first = self.east.patch("acme", "one", changes={"x": 1})
        second = self.east.patch("acme", "two", changes={"x": 1})
        self.assertEqual((first["op_id"], second["op_id"]), ("east:1", "east:2"))

    def test_stats_report_materialized_counts(self) -> None:
        self.east.patch("acme", "one", changes={"a": 1, "b": 2})
        stats = self.east.stats()
        self.assertEqual(stats["operations"], 1)
        self.assertEqual(stats["documents"], 1)
        self.assertEqual(stats["field_versions"], 2)


class CausalityTests(StoreTestCase):
    def test_remote_duplicate_is_idempotent(self) -> None:
        result = self.east.patch("acme", "sku", changes={"price": 1})
        operation = self.east.operation(result["op_id"])
        west = RegionStore(self.root / "west.db", "west")
        self.assertFalse(west.apply_remote(operation)["duplicate"])
        self.assertTrue(west.apply_remote(operation)["duplicate"])
        self.assertEqual(west.stats()["operations"], 1)

    def test_newer_operation_supersedes_older(self) -> None:
        first = self.east.patch("acme", "sku", changes={"price": 1})["op_id"]
        second = self.east.patch("acme", "sku", changes={"price": 2})["op_id"]
        west = RegionStore(self.root / "west.db", "west")
        west.apply_remote(self.east.operation(first))
        west.apply_remote(self.east.operation(second))
        self.assertEqual(west.get_document("acme", "sku")["values"]["price"], 2)

    def test_out_of_order_older_operation_is_obsolete(self) -> None:
        first = self.east.patch("acme", "sku", changes={"price": 1})["op_id"]
        second = self.east.patch("acme", "sku", changes={"price": 2})["op_id"]
        west = RegionStore(self.root / "west.db", "west")
        west.apply_remote(self.east.operation(second))
        result = west.apply_remote(self.east.operation(first))
        self.assertEqual(result["obsolete_fields"], ["price"])
        self.assertEqual(west.get_document("acme", "sku")["values"]["price"], 2)

    def test_concurrent_same_field_surfaces_conflict(self) -> None:
        east, west = self.seed_two_regions()
        east_op = east.patch("acme", "sku", changes={"price": 90})["op_id"]
        west_op = west.patch("acme", "sku", changes={"price": 95})["op_id"]
        east.apply_remote(west.operation(west_op))
        west.apply_remote(east.operation(east_op))
        for store in (east, west):
            document = store.get_document("acme", "sku")
            self.assertEqual(document["conflicts"], ["price"])
            self.assertEqual(len(document["fields"]["price"]["candidates"]), 2)

    def test_concurrent_disjoint_fields_merge(self) -> None:
        east, west = self.seed_two_regions()
        east_op = east.patch("acme", "sku", changes={"price": 90})["op_id"]
        west_op = west.patch("acme", "sku", changes={"status": "backorder"})["op_id"]
        east.apply_remote(west.operation(west_op))
        west.apply_remote(east.operation(east_op))
        for store in (east, west):
            document = store.get_document("acme", "sku")
            self.assertEqual(document["values"], {"price": 90, "status": "backorder"})
            self.assertEqual(document["conflicts"], [])

    def test_concurrent_delete_update_surfaces_conflict(self) -> None:
        east, west = self.seed_two_regions()
        east_op = east.patch("acme", "sku", delete_fields=["status"])["op_id"]
        west_op = west.patch("acme", "sku", changes={"status": "new"})["op_id"]
        east.apply_remote(west.operation(west_op))
        west.apply_remote(east.operation(east_op))
        candidates = east.get_document("acme", "sku")["fields"]["status"]["candidates"]
        self.assertEqual({candidate["deleted"] for candidate in candidates}, {False, True})

    def test_operation_id_collision_rejected(self) -> None:
        original = self.east.patch("acme", "sku", changes={"price": 1})["op_id"]
        collision = Operation.create(
            tenant="other",
            document="different",
            origin="east",
            sequence=1,
            context={},
            changes={"x": 2},
            created_ns=1,
        )
        self.assertEqual(original, collision.op_id)
        with self.assertRaisesRegex(IntegrityError, "collision"):
            self.east.apply_remote(collision)


class ResolutionTests(StoreTestCase):
    def conflict(self) -> tuple[RegionStore, list[str]]:
        east, west = self.seed_two_regions()
        east_id = east.patch("acme", "sku", changes={"price": 90})["op_id"]
        west_id = west.patch("acme", "sku", changes={"price": 95})["op_id"]
        east.apply_remote(west.operation(west_id))
        return west, sorted([east_id, west_id])

    def test_resolution_supersedes_all_candidates(self) -> None:
        _, candidates = self.conflict()
        result = self.east.resolve(
            "acme", "sku", "price", expected_candidates=candidates, value=92
        )
        self.assertEqual(result["document"]["values"]["price"], 92)
        self.assertEqual(result["document"]["conflicts"], [])

    def test_delete_resolution_creates_tombstone(self) -> None:
        _, candidates = self.conflict()
        result = self.east.resolve(
            "acme", "sku", "price", expected_candidates=candidates, delete=True
        )
        self.assertIn("price", result["document"]["deleted_fields"])

    def test_stale_candidate_precondition_rejected(self) -> None:
        _, candidates = self.conflict()
        with self.assertRaisesRegex(ConflictError, "changed"):
            self.east.resolve(
                "acme", "sku", "price", expected_candidates=[*candidates, "west:999"], value=1
            )

    def test_resolution_requires_two_candidates(self) -> None:
        self.east.patch("acme", "sku", changes={"price": 1})
        with self.assertRaises(ValidationError):
            self.east.resolve(
                "acme", "sku", "price", expected_candidates=["east:1"], value=2
            )

    def test_list_conflicts_is_tenant_scoped(self) -> None:
        self.conflict()
        self.assertEqual(len(self.east.list_conflicts(tenant="acme")), 1)
        self.assertEqual(self.east.list_conflicts(tenant="other"), [])


class DeliveryAndIntegrityTests(StoreTestCase):
    def test_export_and_acknowledge(self) -> None:
        self.east.patch("acme", "sku", changes={"x": 1})
        self.assertEqual(len(self.east.export_operations("west")), 1)
        self.east.mark_delivered("west", ["east:1"])
        self.assertEqual(self.east.export_operations("west"), [])

    def test_delivery_ack_is_idempotent(self) -> None:
        self.east.patch("acme", "sku", changes={"x": 1})
        self.east.mark_delivered("west", ["east:1"])
        self.east.mark_delivered("west", ["east:1"])
        self.assertEqual(self.east.stats()["deliveries"], 1)

    def test_ack_unknown_operation_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.east.mark_delivered("west", ["east:999"])

    def test_export_limit_is_bounded(self) -> None:
        with self.assertRaises(ValidationError):
            self.east.export_operations("west", limit=0)

    def test_verify_clean_database(self) -> None:
        self.east.patch("acme", "sku", changes={"x": 1})
        result = self.east.verify()
        self.assertTrue(result["ok"])
        self.assertEqual(result["operations_checked"], 1)

    def test_verify_detects_audit_tampering(self) -> None:
        self.east.patch("acme", "sku", changes={"x": 1})
        connection = sqlite3.connect(self.root / "east.db")
        connection.execute("UPDATE audit_log SET event_json = '{}' WHERE sequence = 1")
        connection.commit()
        connection.close()
        result = self.east.verify()
        self.assertFalse(result["ok"])
        self.assertTrue(any("audit hash" in problem for problem in result["problems"]))

    def test_concurrent_local_patches_are_serialized(self) -> None:
        errors: list[Exception] = []

        def patch(index: int) -> None:
            try:
                self.east.patch("acme", "sku", changes={f"field_{index}": index})
            except Exception as exc:  # pragma: no cover - assertion reports captured failures
                errors.append(exc)

        threads = [threading.Thread(target=patch, args=(index,)) for index in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.east.get_document("acme", "sku")["values"]), 12)
        self.assertEqual(self.east.stats()["operations"], 12)


if __name__ == "__main__":
    unittest.main()
