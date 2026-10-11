import math
import unittest

from surgeguard.client import Policy, retry_after_seconds
from surgeguard.controls import CircuitBreaker, RetryBudget
from surgeguard.transport import validate_url


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class BudgetTests(unittest.TestCase):
    def test_capacity_exhaustion_and_bounded_refill(self):
        clock = Clock()
        bucket = RetryBudget(2, 1, clock=clock)
        self.assertTrue(bucket.take())
        self.assertTrue(bucket.take())
        self.assertFalse(bucket.take())
        clock.now = .5
        self.assertFalse(bucket.take())
        clock.now = 1
        self.assertTrue(bucket.take())
        clock.now = 100
        self.assertTrue(bucket.take())
        self.assertTrue(bucket.take())
        self.assertFalse(bucket.take())

    def test_no_refill_on_backwards_clock(self):
        clock = Clock()
        bucket = RetryBudget(1, 1, clock=clock)
        self.assertTrue(bucket.take())
        clock.now = -10
        self.assertFalse(bucket.take())
        clock.now = .9
        self.assertFalse(bucket.take())

    def test_zero_budget_blocks_retries(self):
        self.assertFalse(RetryBudget(0, 0).take())

    def test_invalid_configuration(self):
        for value in (True, -1, float("nan"), float("inf"), "10"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RetryBudget(value, 1)
        with self.assertRaises(ValueError):
            RetryBudget(1, -1)


class CircuitTests(unittest.TestCase):
    def test_open_single_probe_and_recovery(self):
        clock = Clock()
        circuit = CircuitBreaker(2, 1, clock=clock)
        circuit.complete(circuit.acquire(), healthy=False)
        circuit.complete(circuit.acquire(), healthy=False)
        self.assertEqual(circuit.state, "open")
        self.assertIsNone(circuit.acquire())
        clock.now = 1
        probe = circuit.acquire()
        self.assertTrue(probe.probe)
        self.assertIsNone(circuit.acquire())
        circuit.complete(probe, healthy=True)
        self.assertEqual(circuit.state, "closed")
        self.assertIsNotNone(circuit.acquire())

    def test_old_success_does_not_revive_newly_opened_circuit(self):
        circuit = CircuitBreaker(1)
        old_success = circuit.acquire()
        failure = circuit.acquire()
        circuit.complete(failure, healthy=False)
        circuit.complete(old_success, healthy=True)
        self.assertEqual(circuit.state, "open")
        self.assertIsNone(circuit.acquire())

    def test_failed_probe_reopens_and_cancelled_probe_can_retry(self):
        clock = Clock()
        circuit = CircuitBreaker(1, 1, clock=clock)
        circuit.complete(circuit.acquire(), healthy=False)
        clock.now = 1
        circuit.complete(circuit.acquire(), healthy=False)
        self.assertIsNone(circuit.acquire())
        clock.now = 2
        probe = circuit.acquire()
        circuit.abandon(probe)
        self.assertIsNotNone(circuit.acquire())

    def test_success_resets_consecutive_failures(self):
        circuit = CircuitBreaker(2)
        circuit.complete(circuit.acquire(), healthy=False)
        circuit.complete(circuit.acquire(), healthy=True)
        circuit.complete(circuit.acquire(), healthy=False)
        self.assertEqual(circuit.state, "closed")


class ValidationTests(unittest.TestCase):
    def test_policy_rejects_nonfinite_and_invalid_values(self):
        for kwargs in ({"max_attempts": 0}, {"max_attempts": True}, {"max_attempts": 21}, {"max_inflight": 0}, {"total_timeout": math.nan}, {"attempt_timeout": 0}, {"base_delay": -1}, {"base_delay": .1, "max_delay": .01}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Policy(**kwargs)

    def test_retry_after_numeric_date_and_invalid(self):
        self.assertEqual(retry_after_seconds("10"), 10)
        self.assertEqual(retry_after_seconds("Thu, 01 Jan 1970 00:00:10 GMT", now=5), 5)
        self.assertEqual(retry_after_seconds("Thu, 01 Jan 1970 00:00:10 GMT", now=20), 0)
        self.assertTrue(math.isinf(retry_after_seconds("9" * 100)))
        for value in (None, "", "nonsense", "-1", "NaN", "1.5", "١"):
            self.assertEqual(retry_after_seconds(value), 0)

    def test_loopback_only_prevents_accidental_external_calls(self):
        for url in ("http://example.com/", "http://169.254.169.254/", "http://192.168.1.1/", "http://localhost/"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                validate_url(url)
        validate_url("http://127.0.0.1:8080/work")
        validate_url("http://[::1]:8080/work")
        validate_url("https://example.com/", allow_network=True)

    def test_credentials_injection_fragments_and_bad_ports_rejected(self):
        for url in ("file:///etc/passwd", "http://user:pass@127.0.0.1/", "http://127.0.0.1/#token", "http://127.0.0.1:0/", "http://127.0.0.1:99999/", "http://127.0.0.1/\r\nX:evil", "http://127.0.0.1/with space"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                validate_url(url)
