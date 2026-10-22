from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
from pathlib import Path

from mergemesh.model import IntegrityError, ValidationError
from mergemesh.store import RegionStore
from mergemesh.sync import (
    converge,
    deliver_batch,
    receive_envelope,
    sign_operation,
    verify_envelope,
    whole_document_lww,
)


class SyncTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.east = RegionStore(root / "east.db", "east")
        self.west = RegionStore(root / "west.db", "west")
        self.central = RegionStore(root / "central.db", "central")
        self.secrets = {
            region: hashlib.sha256(f"test:{region}".encode()).digest()
            for region in ("east", "west", "central")
        }
        op_id = self.east.patch("acme", "sku", changes={"price": 1})["op_id"]
        self.operation = self.east.operation(op_id)

    def tearDown(self) -> None:
        self.temporary.cleanup()


class EnvelopeTests(SyncTestCase):
    def test_sign_and_verify(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        self.assertEqual(verify_envelope(envelope, self.secrets), self.operation)

    def test_short_secret_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            sign_operation(self.operation, b"short")

    def test_payload_tampering_rejected(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        envelope["operation"]["mutations"]["price"]["value"] = 999
        with self.assertRaisesRegex(IntegrityError, "signature"):
            verify_envelope(envelope, self.secrets)

    def test_signature_tampering_rejected(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        envelope["signature"] = "0" * 64
        with self.assertRaises(IntegrityError):
            verify_envelope(envelope, self.secrets)

    def test_unknown_key_rejected(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        with self.assertRaisesRegex(IntegrityError, "unknown"):
            verify_envelope(envelope, {})

    def test_key_cannot_claim_another_origin(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        forged = copy.deepcopy(envelope)
        forged["key_id"] = "west"
        signed = {key: forged[key] for key in ("schema", "algorithm", "key_id", "operation")}
        import hmac

        from mergemesh.model import canonical_json

        forged["signature"] = hmac.new(
            self.secrets["west"], canonical_json(signed).encode(), hashlib.sha256
        ).hexdigest()
        with self.assertRaisesRegex(IntegrityError, "not authorized"):
            verify_envelope(forged, self.secrets)

    def test_unknown_envelope_field_rejected(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        envelope["extra"] = True
        with self.assertRaises(ValidationError):
            verify_envelope(envelope, self.secrets)

    def test_wrong_algorithm_rejected(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        envelope["algorithm"] = "none"
        with self.assertRaises(ValidationError):
            verify_envelope(envelope, self.secrets)

    def test_receive_persists_verified_operation(self) -> None:
        envelope = sign_operation(self.operation, self.secrets["east"])
        result = receive_envelope(self.west, envelope, self.secrets)
        self.assertFalse(result["duplicate"])
        self.assertEqual(self.west.get_document("acme", "sku")["values"]["price"], 1)


class DeliveryTests(SyncTestCase):
    def test_batch_delivers_and_acknowledges(self) -> None:
        result = deliver_batch(self.east, self.west, self.secrets)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(self.east.export_operations("west"), [])

    def test_duplicate_injection_is_suppressed(self) -> None:
        result = deliver_batch(
            self.east, self.west, self.secrets, duplicate_every=1
        )
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["duplicates"], 1)
        self.assertEqual(self.west.stats()["operations"], 1)

    def test_lost_ack_retries_committed_operation(self) -> None:
        first = deliver_batch(self.east, self.west, self.secrets, acknowledge=False)
        second = deliver_batch(self.east, self.west, self.secrets)
        self.assertEqual(first["accepted"], 1)
        self.assertEqual(second["duplicates"], 1)
        self.assertEqual(self.west.stats()["operations"], 1)

    def test_reverse_delivery_handles_causal_reordering(self) -> None:
        self.east.patch("acme", "sku", changes={"price": 2})
        result = deliver_batch(self.east, self.west, self.secrets, order="reverse")
        self.assertEqual(result["accepted"], 2)
        self.assertEqual(self.west.get_document("acme", "sku")["values"]["price"], 2)

    def test_shuffle_delivery_is_deterministic_and_convergent(self) -> None:
        for value in range(2, 8):
            self.east.patch("acme", "sku", changes={"price": value})
        result = deliver_batch(
            self.east, self.west, self.secrets, order="shuffle", random_seed=42
        )
        self.assertEqual(result["accepted"], 7)
        self.assertEqual(self.west.get_document("acme", "sku")["values"]["price"], 7)

    def test_invalid_order_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            deliver_batch(self.east, self.west, self.secrets, order="random")

    def test_cannot_deliver_to_same_region(self) -> None:
        with self.assertRaises(ValidationError):
            deliver_batch(self.east, self.east, self.secrets)

    def test_three_region_convergence(self) -> None:
        deliver_batch(self.east, self.west, self.secrets)
        self.west.patch("acme", "sku", changes={"status": "ready"})
        self.central.patch("acme", "other", changes={"x": 3})
        result = converge([self.east, self.west, self.central], self.secrets)
        self.assertTrue(result["converged"])
        for store in (self.east, self.west, self.central):
            self.assertEqual(store.get_document("acme", "sku")["values"]["status"], "ready")
            self.assertEqual(store.get_document("acme", "other")["values"]["x"], 3)

    def test_transitive_operation_is_forwarded(self) -> None:
        deliver_batch(self.east, self.west, self.secrets)
        self.assertEqual(self.central.stats()["operations"], 0)
        deliver_batch(self.west, self.central, self.secrets)
        self.assertEqual(self.central.get_document("acme", "sku")["values"]["price"], 1)

    def test_duplicate_region_in_converge_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            converge([self.east, self.east], self.secrets)


class LwwBaselineTests(unittest.TestCase):
    def test_highest_timestamp_wins_complete_document(self) -> None:
        result = whole_document_lww(
            [
                {"timestamp": 1, "region": "east", "document": {"a": 2, "b": 1}},
                {"timestamp": 2, "region": "west", "document": {"a": 1, "b": 2}},
            ]
        )
        self.assertEqual(result["document"], {"a": 1, "b": 2})

    def test_region_breaks_timestamp_tie_deterministically(self) -> None:
        result = whole_document_lww(
            [
                {"timestamp": 1, "region": "east", "document": {"x": 1}},
                {"timestamp": 1, "region": "west", "document": {"x": 2}},
            ]
        )
        self.assertEqual(result["region"], "west")

    def test_empty_revisions_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            whole_document_lww([])

    def test_boolean_timestamp_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            whole_document_lww([{"timestamp": True, "region": "east", "document": {}}])


if __name__ == "__main__":
    unittest.main()
