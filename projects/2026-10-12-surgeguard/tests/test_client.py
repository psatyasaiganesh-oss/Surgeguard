import asyncio
import random
import time
import unittest

from surgeguard import CircuitBreaker, Policy, ResilientClient, Response, RetryBudget
from surgeguard.transport import TransportError


URL = "http://127.0.0.1:8090/work"


def configured(**kwargs):
    return Policy(**{**dict(total_timeout=.3, attempt_timeout=.1, base_delay=0, max_delay=0, max_inflight=100), **kwargs})


class Sequence:
    def __init__(self, *events):
        self.events = list(events)
        self.calls = 0

    async def __call__(self, url, method):
        value = self.events[min(self.calls, len(self.events) - 1)]
        self.calls += 1
        if isinstance(value, BaseException):
            raise value
        return value


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_failure_recovers_on_second_attempt(self):
        transport = Sequence(Response(503), Response(200))
        client = ResilientClient(configured(), transport=transport)
        result = await client.fetch(URL)
        self.assertTrue(result.ok)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(client.snapshot()["counts"]["retries"], 1)

    async def test_only_selected_status_codes_are_retried(self):
        for status in (400, 401, 403, 404, 409, 422, 501, 302):
            transport = Sequence(Response(status), Response(200))
            result = await ResilientClient(configured(), transport=transport).fetch(URL)
            self.assertFalse(result.ok)
            self.assertEqual(transport.calls, 1)
        for status in (408, 429, 500, 502, 503, 504):
            result = await ResilientClient(configured(), transport=Sequence(Response(status), Response(200))).fetch(URL)
            self.assertTrue(result.ok)
            self.assertEqual(result.attempts, 2)

    async def test_network_failure_can_recover(self):
        result = await ResilientClient(configured(), transport=Sequence(ConnectionResetError(), Response(200))).fetch(URL)
        self.assertTrue(result.ok)
        self.assertEqual(result.attempts, 2)

    async def test_protocol_errors_are_not_retried(self):
        transport = Sequence(TransportError("bad framing"))
        result = await ResilientClient(configured(), transport=transport).fetch(URL)
        self.assertEqual(result.reason, "protocol_error")
        self.assertEqual(transport.calls, 1)

    async def test_writes_rejected_before_transport(self):
        transport = Sequence(Response(200))
        with self.assertRaises(ValueError):
            await ResilientClient(transport=transport).fetch(URL, method="POST")
        self.assertEqual(transport.calls, 0)

    async def test_attempt_cap(self):
        transport = Sequence(Response(503))
        result = await ResilientClient(configured(max_attempts=3), transport=transport, unbounded_retries=True).fetch(URL)
        self.assertEqual(result.reason, "attempts_exhausted")
        self.assertEqual(transport.calls, 3)

    async def test_zero_budget_still_permits_original_attempt(self):
        transport = Sequence(Response(503))
        result = await ResilientClient(configured(), transport=transport, budget=RetryBudget(0, 0)).fetch(URL)
        self.assertEqual(result.reason, "retry_budget_exhausted")
        self.assertEqual(result.attempts, 1)

    async def test_concurrent_outage_obeys_shared_budget(self):
        transport = Sequence(Response(503))
        client = ResilientClient(configured(), transport=transport, budget=RetryBudget(7, 0))
        outcomes = await asyncio.gather(*(client.fetch(URL) for _ in range(40)))
        self.assertTrue(all(not outcome.ok for outcome in outcomes))
        self.assertEqual(transport.calls, 47)
        self.assertEqual(client.snapshot()["counts"]["retries"], 7)

    async def test_bulkhead_rejects_before_downstream_call_and_releases(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = 0
        async def slow(url, method):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return Response(200)
        client = ResilientClient(configured(max_inflight=1), transport=slow)
        pending = asyncio.create_task(client.fetch(URL))
        await entered.wait()
        rejected = await client.fetch(URL)
        self.assertEqual(rejected.reason, "bulkhead_full")
        self.assertEqual(rejected.attempts, 0)
        release.set()
        self.assertTrue((await pending).ok)
        self.assertTrue((await client.fetch(URL)).ok)
        self.assertEqual(calls, 2)
        self.assertEqual(client.active, 0)

    async def test_deadline_cancels_slow_adapter(self):
        cancelled = asyncio.Event()
        async def slow(url, method):
            try:
                await asyncio.sleep(1)
            finally:
                cancelled.set()
        client = ResilientClient(configured(total_timeout=.03, attempt_timeout=1), transport=slow)
        result = await client.fetch(URL)
        self.assertEqual(result.reason, "deadline_exhausted")
        self.assertTrue(cancelled.is_set())
        self.assertLess(result.elapsed_s, .2)
        self.assertEqual(client.active, 0)

    async def test_per_attempt_timeout_can_recover_within_total_deadline(self):
        calls = 0
        async def sometimes_slow(url, method):
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.sleep(1)
            return Response(200)
        result = await ResilientClient(configured(attempt_timeout=.01), transport=sometimes_slow).fetch(URL)
        self.assertTrue(result.ok)
        self.assertEqual(calls, 2)

    async def test_retry_after_is_not_shortened_to_fit_deadline(self):
        transport = Sequence(Response(429, {"retry-after": "1"}), Response(200))
        result = await ResilientClient(configured(total_timeout=.05), transport=transport).fetch(URL)
        self.assertEqual(result.reason, "retry_would_exceed_deadline")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(transport.calls, 1)

    async def test_cancellation_frees_slot_and_probe(self):
        circuit = CircuitBreaker(threshold=1, cooldown=.001)
        circuit.complete(circuit.acquire(), healthy=False)
        await asyncio.sleep(.002)
        entered = asyncio.Event()
        async def slow(url, method):
            entered.set()
            await asyncio.sleep(10)
            return Response(200)
        client = ResilientClient(configured(), transport=slow, circuit=circuit)
        pending = asyncio.create_task(client.fetch(URL))
        await entered.wait()
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertEqual(client.active, 0)
        self.assertEqual(client.snapshot()["counts"]["cancelled"], 1)
        self.assertIsNotNone(circuit.acquire())

    async def test_client_error_does_not_open_circuit(self):
        circuit = CircuitBreaker(threshold=1)
        client = ResilientClient(configured(), transport=Sequence(Response(404)), circuit=circuit)
        result = await client.fetch(URL)
        self.assertFalse(result.ok)
        self.assertEqual(circuit.state, "closed")

    async def test_breaker_blocks_without_making_extra_call(self):
        circuit = CircuitBreaker(threshold=1)
        transport = Sequence(Response(503))
        client = ResilientClient(configured(max_attempts=1), transport=transport, circuit=circuit)
        await client.fetch(URL)
        result = await client.fetch(URL)
        self.assertEqual(result.reason, "circuit_open")
        self.assertEqual(transport.calls, 1)

    async def test_full_jitter_spreads_retry_delays(self):
        class TracingRandom(random.Random):
            def __init__(self):
                super().__init__(7)
                self.draws = []

            def uniform(self, low, high):
                value = super().uniform(low, high)
                self.draws.append((low, high, value))
                return value
        # Real sleeps, but no narrow wall-clock upper bound that would make CI
        # failures depend on unrelated host load.
        rng = TracingRandom()
        timestamps = []
        async def flaky(url, method):
            timestamps.append(time.monotonic())
            return Response(503)
        policy = configured(max_attempts=4, total_timeout=.5, base_delay=.02, max_delay=.02)
        result = await ResilientClient(policy, transport=flaky, unbounded_retries=True, rng=rng).fetch(URL)
        self.assertEqual(result.attempts, 4)
        self.assertEqual(len(rng.draws), 3)
        self.assertTrue(all(low == 0 and high == .02 and 0 <= value <= .02 for low, high, value in rng.draws))
        self.assertEqual(len(set(value for _, _, value in rng.draws)), 3)
        self.assertGreaterEqual(timestamps[-1] - timestamps[0], sum(value for _, _, value in rng.draws) - .002)

    async def test_success_and_failure_latency_are_separate(self):
        client = ResilientClient(configured(), transport=Sequence(Response(404), Response(200)))
        await client.fetch(URL)
        await client.fetch(URL)
        metrics = client.snapshot()
        self.assertEqual(metrics["latency"]["success"]["samples"], 1)
        self.assertEqual(metrics["latency"]["failure"]["samples"], 1)
        self.assertEqual(metrics["counts"]["requests"], 2)
        self.assertEqual(metrics["counts"]["completed"], 2)

    async def test_adapter_programming_error_is_visible_and_releases_slot(self):
        client = ResilientClient(configured(), transport=Sequence(RuntimeError("adapter bug")))
        with self.assertRaisesRegex(RuntimeError, "adapter bug"):
            await client.fetch(URL)
        self.assertEqual(client.active, 0)
