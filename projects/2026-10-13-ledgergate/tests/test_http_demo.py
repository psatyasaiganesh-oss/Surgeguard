import http.client
import json
import tempfile
import time
import unittest
from pathlib import Path

from ledgergate import LedgerGate, SQLiteStore, sign_payload
from ledgergate.demo import run_demo
from ledgergate.httpserver import ServerRuntime

from tests.helpers import SECRET, make_payload


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.directory.name) / "http.db")
        self.runtime = ServerRuntime(LedgerGate(self.store, [SECRET])).start()
        self.host, self.port = self.runtime.address

    def tearDown(self):
        self.runtime.close()
        self.store.close()
        self.directory.cleanup()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection(self.host, self.port, timeout=3)
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_health_metrics_and_not_found(self):
        self.assertEqual(self.request("GET", "/health"), (200, {"status": "ok"}))
        status, metrics = self.request("GET", "/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(metrics["payments"], 0)
        self.assertEqual(self.request("GET", "/missing"), (404, {"error": "not_found"}))

    def test_webhook_is_authenticated_and_processed_asynchronously(self):
        raw = make_payload()
        signature = sign_payload(raw, SECRET, int(time.time()))
        status, receipt = self.request(
            "POST",
            "/webhook",
            raw,
            {"Content-Type": "application/json", "Stripe-Signature": signature},
        )
        self.assertEqual((status, receipt["status"]), (202, "accepted"))

        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and self.store.snapshot()["sink_unique_deliveries"] != 1:
            time.sleep(0.01)
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["payments"], 1)
        self.assertEqual(snapshot["sink_unique_deliveries"], 1)

        status, receipt = self.request(
            "POST",
            "/webhook",
            raw,
            {"Content-Type": "application/json", "Stripe-Signature": signature},
        )
        self.assertEqual((status, receipt["status"]), (200, "duplicate_event"))

    def test_invalid_signature_and_protocol_limits_are_rejected(self):
        raw = make_payload()
        status, receipt = self.request(
            "POST",
            "/webhook",
            raw,
            {"Stripe-Signature": "t=1,v1=" + "0" * 64},
        )
        self.assertEqual((status, receipt["status"]), (400, "invalid_signature"))

        status, result = self.request("POST", "/webhook", b"", {"Content-Length": "0"})
        self.assertEqual((status, result["error"]), (413, "payload_size_out_of_range"))
        self.assertEqual(self.request("POST", "/missing", b"x"), (404, {"error": "not_found"}))

    def test_runtime_rejects_non_loopback_binding(self):
        with self.assertRaises(ValueError):
            ServerRuntime(LedgerGate(self.store, [SECRET]), host="0.0.0.0")


class DemoTests(unittest.TestCase):
    def test_deterministic_demo_proves_all_invariants(self):
        report = run_demo(count=40, seed=13)
        self.assertTrue(all(report["invariants"].values()))
        self.assertEqual(report["ledgergate"]["final_state_mismatches"], 0)
        self.assertGreater(report["naive_authenticated_baseline"]["final_state_mismatches"], 0)
        self.assertEqual(report["crash_recovery"]["recovered"]["sink_unique_deliveries"], 1)

    def test_demo_validates_arguments(self):
        for invalid in (9, True, 20_001):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                run_demo(count=invalid)
        with self.assertRaises(ValueError):
            run_demo(seed=True)


if __name__ == "__main__":
    unittest.main()
