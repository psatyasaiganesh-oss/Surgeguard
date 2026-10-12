import concurrent.futures
import tempfile
import unittest
from pathlib import Path

from ledgergate import LedgerGate, SQLiteStore

from tests.helpers import NOW, SECRET, make_payload, signed


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.directory.name) / "test.db")
        self.service = LedgerGate(self.store, [SECRET])

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def accept(self, raw):
        return self.service.accept(raw, signed(raw), now=NOW)

    def test_secure_intake_acknowledges_without_business_processing(self):
        raw = make_payload()
        receipt = self.accept(raw)
        self.assertEqual((receipt.http_status, receipt.result), (202, "accepted"))
        self.assertIsNone(self.store.payment("pi_test_0001"))
        self.assertEqual(self.store.snapshot()["events"], {"pending": 1})

    def test_invalid_signature_and_payload_do_not_enter_inbox(self):
        raw = make_payload()
        self.assertEqual(self.service.accept(raw, f"t={NOW},v1={'0'*64}", now=NOW).result, "invalid_signature")
        bad = b"{}"
        self.assertEqual(self.service.accept(bad, signed(bad), now=NOW).result, "invalid_payload")
        self.assertEqual(self.store.snapshot()["events"], {})

    def test_exact_duplicate_semantic_duplicate_and_conflicting_id(self):
        raw = make_payload()
        self.assertEqual(self.accept(raw).result, "accepted")
        self.assertEqual(self.accept(raw).result, "duplicate_event")
        semantic = make_payload(event_id="evt_test_0002")
        self.assertEqual(self.accept(semantic).result, "duplicate_effect")
        conflict = make_payload(amount=9999)
        receipt = self.accept(conflict)
        self.assertEqual((receipt.http_status, receipt.result), (409, "event_conflict"))
        self.assertEqual(self.store.audit_results(), {"accepted": 1, "duplicate_effect": 1, "duplicate_event": 1, "event_conflict": 1})

    def test_concurrent_duplicate_delivery_admits_once(self):
        raw = make_payload()
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            results = list(executor.map(lambda _: self.accept(raw).result, range(40)))
        self.assertEqual(results.count("accepted"), 1)
        self.assertEqual(results.count("duplicate_event"), 39)

    def test_out_of_order_delivery_does_not_regress_state(self):
        succeeded = make_payload(event_id="evt_final_0001")
        created = make_payload(event_id="evt_created_0001", event_type="payment_intent.created", version=1, status="created")
        self.accept(succeeded)
        self.accept(created)
        self.assertEqual(self.store.process_all(now=NOW), 2)
        payment = self.store.payment("pi_test_0001")
        self.assertEqual((payment["resource_version"], payment["status"]), (2, "succeeded"))
        self.assertEqual(self.store.snapshot()["events"], {"processed": 1, "stale": 1})

    def test_same_version_conflicting_state_is_quarantined(self):
        succeeded = make_payload(event_id="evt_success_0001")
        failed = make_payload(event_id="evt_failed_0001", event_type="payment_intent.payment_failed", status="failed")
        self.accept(succeeded)
        self.accept(failed)
        self.store.process_all(now=NOW)
        self.assertEqual(self.store.payment("pi_test_0001")["status"], "succeeded")
        self.assertEqual(self.store.snapshot()["events"], {"conflict": 1, "processed": 1})

    def test_transaction_rollback_recovers_without_partial_projection(self):
        self.accept(make_payload())
        with self.assertRaisesRegex(RuntimeError, "injected crash"):
            self.store.process_one(now=NOW, crash_before_commit=True)
        self.assertIsNone(self.store.payment("pi_test_0001"))
        self.assertEqual(self.store.snapshot()["outbox"], {})
        self.assertEqual(self.store.snapshot()["events"], {"pending": 1})
        self.store.process_one(now=NOW)
        self.assertEqual(self.store.payment("pi_test_0001")["status"], "succeeded")

    def test_outbox_retry_uses_receiver_idempotency_key(self):
        self.accept(make_payload())
        self.store.process_one(now=NOW)
        with self.assertRaisesRegex(RuntimeError, "after sink"):
            self.store.dispatch_one(now=NOW, lease_seconds=0, crash_after_sink=True)
        self.assertEqual(self.store.snapshot()["sink_unique_deliveries"], 1)
        self.store.dispatch_one(now=NOW, lease_seconds=0)
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["sink_unique_deliveries"], 1)
        self.assertEqual(snapshot["sink_delivery_attempts"], 2)
        self.assertEqual(snapshot["outbox"], {"delivered": 1})

    def test_failed_payment_never_creates_fulfillment(self):
        raw = make_payload(event_type="payment_intent.payment_failed", status="failed")
        self.accept(raw)
        result = self.service.drain(now=NOW)
        self.assertEqual(result["snapshot"]["sink_unique_deliveries"], 0)

    def test_in_memory_store_is_supported(self):
        store = SQLiteStore(":memory:")
        service = LedgerGate(store, [SECRET])
        raw = make_payload()
        self.assertEqual(service.accept(raw, signed(raw), now=NOW).result, "accepted")
        self.assertEqual(service.drain(now=NOW)["snapshot"]["sink_unique_deliveries"], 1)
        store.close()

    def test_limits_are_validated(self):
        for method in (self.store.process_all, self.store.dispatch_all):
            for value in (0, True, 1_000_001):
                with self.subTest(method=method.__name__, value=value), self.assertRaises(ValueError):
                    method(limit=value)
        with self.assertRaises(ValueError):
            self.store.dispatch_one(lease_seconds=-1)
