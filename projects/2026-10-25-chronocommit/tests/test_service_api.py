from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

from chronocommit.api import build_server
from chronocommit.clock import ManualBoundedClock
from chronocommit.coordinator import Coordinator
from chronocommit.errors import (
    ConflictError,
    IdempotencyConflict,
    InsufficientFunds,
    NotFoundError,
    ValidationError,
)
from chronocommit.service import AccountService, MAX_BALANCE_CENTS


SECRET = b"service-api-test-decision-secret-32bytes"
TOKEN = "service-api-token-that-is-longer-than-32-bytes"


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.clock = ManualBoundedClock(2_000_000, 5)
        self.coordinator = Coordinator(
            self.temp.name, shard_count=3, secret=SECRET, clock=self.clock
        )
        self.service = AccountService(self.coordinator)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def open_pair(self) -> None:
        self.service.create_account("tenant", "checking", 10_000, request_id="open-c")
        self.service.create_account("tenant", "savings", 5_000, request_id="open-s")

    def test_create_and_get_account(self) -> None:
        result = self.service.create_account(
            "tenant", "checking", 10_000, request_id="open-c"
        )
        account = self.service.get_accounts("tenant", ["checking"])["checking"]
        self.assertEqual(account["balance_cents"], 10_000)
        self.assertEqual(account["commit_ts"], result.commit_ts)

    def test_create_retry_is_idempotent(self) -> None:
        first = self.service.create_account(
            "tenant", "checking", 10_000, request_id="open-c"
        )
        replay = self.service.create_account(
            "tenant", "checking", 10_000, request_id="open-c"
        )
        self.assertEqual(first.txn_id, replay.txn_id)
        self.assertTrue(replay.replayed)

    def test_create_request_id_cannot_change_account(self) -> None:
        self.service.create_account("tenant", "checking", 1, request_id="same")
        with self.assertRaises(IdempotencyConflict):
            self.service.create_account("tenant", "savings", 1, request_id="same")

    def test_transfer_conserves_total(self) -> None:
        self.open_pair()
        result = self.service.transfer(
            "tenant", "transfer-1", "checking", "savings", 2_500
        )
        self.assertEqual(result["balances"]["checking"]["balance_cents"], 7_500)
        self.assertEqual(result["balances"]["savings"]["balance_cents"], 7_500)
        self.assertEqual(self.service.total_balance("tenant", ["checking", "savings"]), 15_000)

    def test_transfer_replay_uses_original_commit_snapshot(self) -> None:
        self.open_pair()
        first = self.service.transfer(
            "tenant", "first", "checking", "savings", 1_000
        )
        self.service.transfer("tenant", "later", "checking", "savings", 500)
        replay = self.service.transfer(
            "tenant", "first", "checking", "savings", 1_000
        )
        self.assertTrue(replay["commit"]["replayed"])
        self.assertEqual(replay["commit"]["txn_id"], first["commit"]["txn_id"])
        self.assertEqual(replay["balances"]["checking"]["balance_cents"], 9_000)
        self.assertEqual(self.service.get_accounts("tenant", ["checking"])["checking"]["balance_cents"], 8_500)

    def test_transfer_id_rejects_changed_amount(self) -> None:
        self.open_pair()
        self.service.transfer("tenant", "same", "checking", "savings", 100)
        with self.assertRaises(IdempotencyConflict):
            self.service.transfer("tenant", "same", "checking", "savings", 200)

    def test_insufficient_funds_is_rejected_without_writes(self) -> None:
        self.open_pair()
        with self.assertRaises(InsufficientFunds):
            self.service.transfer("tenant", "too-large", "checking", "savings", 20_000)
        self.assertEqual(self.service.total_balance("tenant", ["checking", "savings"]), 15_000)

    def test_missing_account_is_rejected(self) -> None:
        self.service.create_account("tenant", "checking", 1, request_id="open")
        with self.assertRaises(NotFoundError):
            self.service.transfer("tenant", "missing", "checking", "absent", 1)

    def test_same_source_and_destination_is_rejected(self) -> None:
        self.service.create_account("tenant", "checking", 1, request_id="open")
        with self.assertRaises(ValidationError):
            self.service.transfer("tenant", "same", "checking", "checking", 1)

    def test_boolean_amount_is_rejected(self) -> None:
        self.open_pair()
        with self.assertRaises(ValidationError):
            self.service.transfer("tenant", "bool", "checking", "savings", True)

    def test_destination_overflow_is_rejected(self) -> None:
        self.service.create_account("tenant", "source", 1, request_id="source")
        self.service.create_account(
            "tenant", "destination", MAX_BALANCE_CENTS, request_id="destination"
        )
        with self.assertRaises(ConflictError):
            self.service.transfer("tenant", "overflow", "source", "destination", 1)


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        coordinator = Coordinator(
            Path(self.temp.name),
            shard_count=3,
            secret=SECRET,
            clock=ManualBoundedClock(3_000_000, 5),
        )
        self.server = build_server(
            "127.0.0.1",
            0,
            coordinator=coordinator,
            tenant="tenant",
            api_token=TOKEN,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temp.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        token: str | None = TOKEN,
        raw: bytes | None = None,
        content_type: str = "application/json",
    ) -> tuple[int, dict[str, Any], dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        data: bytes | None = raw
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        if data is not None:
            headers["Content-Type"] = content_type
            headers["Content-Length"] = str(len(data))
        connection.request(method, path, body=data, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, payload, response_headers

    def create_accounts(self) -> None:
        for account, balance, request_id in (
            ("checking", 10_000, "open-c"),
            ("savings", 5_000, "open-s"),
        ):
            status, _, _ = self.request(
                "POST",
                "/v1/accounts",
                {"account": account, "balance_cents": balance, "request_id": request_id},
            )
            self.assertEqual(status, 201)

    def test_health_is_public_and_runs_integrity_check(self) -> None:
        status, payload, headers = self.request("GET", "/healthz", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(headers["cache-control"], "no-store")

    def test_protected_route_requires_token(self) -> None:
        status, payload, _ = self.request("GET", "/v1/accounts?ids=checking", token=None)
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "unauthorized")

    def test_account_create_and_read(self) -> None:
        self.create_accounts()
        status, payload, headers = self.request(
            "GET", "/v1/accounts?ids=checking,savings"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["accounts"]["checking"]["balance_cents"], 10_000)
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_transfer_and_status(self) -> None:
        self.create_accounts()
        status, payload, _ = self.request(
            "POST",
            "/v1/transfers",
            {
                "amount_cents": 1_000,
                "destination": "savings",
                "source": "checking",
                "transfer_id": "api-1",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["balances"]["checking"]["balance_cents"], 9_000)
        status, payload, _ = self.request(
            "GET", "/v1/transactions/transfer-api-1"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["transaction"]["state"], "COMMITTED")

    def test_transfer_retry_does_not_double_debit(self) -> None:
        self.create_accounts()
        request = {
            "amount_cents": 1_000,
            "destination": "savings",
            "source": "checking",
            "transfer_id": "api-retry",
        }
        first = self.request("POST", "/v1/transfers", request)[1]
        replay = self.request("POST", "/v1/transfers", request)[1]
        self.assertEqual(first["commit"]["txn_id"], replay["commit"]["txn_id"])
        self.assertTrue(replay["commit"]["replayed"])

    def test_duplicate_json_key_is_rejected(self) -> None:
        raw = b'{"account":"a","account":"b","balance_cents":1,"request_id":"x"}'
        status, payload, _ = self.request("POST", "/v1/accounts", raw=raw)
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "validation_error")

    def test_unknown_field_is_rejected(self) -> None:
        status, payload, _ = self.request(
            "POST",
            "/v1/accounts",
            {"account": "a", "balance_cents": 1, "request_id": "x", "tenant": "other"},
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "validation_error")

    def test_wrong_content_type_is_rejected(self) -> None:
        raw = b'{"account":"a","balance_cents":1,"request_id":"x"}'
        status, _, _ = self.request(
            "POST", "/v1/accounts", raw=raw, content_type="text/plain"
        )
        self.assertEqual(status, 400)

    def test_missing_transaction_is_404(self) -> None:
        status, payload, _ = self.request("GET", "/v1/transactions/missing")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "not_found")

    def test_bad_account_query_is_rejected(self) -> None:
        status, _, _ = self.request("GET", "/v1/accounts")
        self.assertEqual(status, 400)

    def test_server_rejects_non_loopback_and_short_token(self) -> None:
        coordinator = self.server.coordinator
        with self.assertRaises(ValidationError):
            build_server(
                "0.0.0.0", 0, coordinator=coordinator, tenant="tenant", api_token=TOKEN
            )
        with self.assertRaises(ValidationError):
            build_server(
                "127.0.0.1", 0, coordinator=coordinator, tenant="tenant", api_token="short"
            )


if __name__ == "__main__":
    unittest.main()

