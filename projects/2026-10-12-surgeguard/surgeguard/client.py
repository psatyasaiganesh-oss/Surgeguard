"""A single retry layer with a total deadline and a bounded bulkhead."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Awaitable, Callable

from .controls import CircuitBreaker, RetryBudget, positive_number
from .metrics import Metrics
from .transport import HttpTransport, Response, TransportError


RETRYABLE = frozenset({408, 429, 500, 502, 503, 504})


def retry_after_seconds(value: str | None, *, now: float | None = None) -> float:
    if not value:
        return 0.0
    if value.isascii() and value.isdigit():
        # Very large server delays need no large integer conversion.
        if len(value) > 9:
            return float("inf")
        return float(value)
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            return 0.0
        return max(0.0, date.timestamp() - (time.time() if now is None else now))
    except (ValueError, TypeError, OverflowError):
        return 0.0


@dataclass(frozen=True)
class Policy:
    max_attempts: int = 4
    total_timeout: float = 1.0
    attempt_timeout: float = .25
    base_delay: float = .01
    max_delay: float = .1
    max_inflight: int = 32

    def __post_init__(self):
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 20:
            raise ValueError("max_attempts must be an integer in [1, 20]")
        if type(self.max_inflight) is not int or not 1 <= self.max_inflight <= 10000:
            raise ValueError("max_inflight must be an integer in [1, 10000]")
        for key in ("total_timeout", "attempt_timeout"):
            positive_number(key, getattr(self, key))
        for key in ("base_delay", "max_delay"):
            positive_number(key, getattr(self, key), allow_zero=True)
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be at least base_delay")


@dataclass(frozen=True)
class Outcome:
    ok: bool
    reason: str
    attempts: int
    elapsed_s: float
    response: Response | None = None

    def to_dict(self) -> dict:
        return {"ok": self.ok, "reason": self.reason, "attempts": self.attempts, "elapsed_ms": round(self.elapsed_s * 1000, 3), "status": self.response.status if self.response else None}


@dataclass
class _Progress:
    attempts: int = 0
    response: Response | None = None


class ResilientClient:
    """Use one instance per dependency on one event loop.

    Circuit breaking is optional and disabled by default. A supplied None
    retry budget is an explicitly unbounded baseline used only by the demo.
    """

    def __init__(self, policy: Policy | None = None, *, transport: Callable[[str, str], Awaitable[Response]] | None = None, budget: RetryBudget | None = None, unbounded_retries: bool = False, circuit: CircuitBreaker | None = None, rng: random.Random | None = None):
        self.policy = policy or Policy()
        self.transport = transport or HttpTransport()
        if unbounded_retries and budget is not None:
            raise ValueError("Choose either an explicit budget or unbounded baseline")
        self.budget = None if unbounded_retries else (budget if budget is not None else RetryBudget())
        self.circuit = circuit
        self.rng = rng or random.Random()
        self.metrics = Metrics()
        self.active = 0
        self._loop = None

    async def fetch(self, url: str, *, method: str = "GET") -> Outcome:
        if method not in {"GET", "HEAD"}:
            raise ValueError("Automatic retries are restricted to GET/HEAD; writes require a separate idempotency design")
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("ResilientClient cannot be shared across event loops")
        self._loop = loop
        validator = getattr(self.transport, "validate", None)
        if validator:
            validator(url)
        start = time.monotonic()
        self.metrics.counts["requests"] += 1
        if self.active >= self.policy.max_inflight:
            self.metrics.counts["bulkhead_rejected"] += 1
            outcome = Outcome(False, "bulkhead_full", 0, time.monotonic() - start)
            self.metrics.record(outcome)
            return outcome
        permit = self.circuit.acquire() if self.circuit else None
        if self.circuit and permit is None:
            self.metrics.counts["circuit_rejected"] += 1
            outcome = Outcome(False, "circuit_open", 0, time.monotonic() - start)
            self.metrics.record(outcome)
            return outcome
        self.active += 1
        self.metrics.max_active = max(self.metrics.max_active, self.active)
        progress = _Progress()
        try:
            try:
                reason, healthy = await asyncio.wait_for(self._execute(url, method, start, progress), timeout=self.policy.total_timeout)
            except TimeoutError:
                reason, healthy = "deadline_exhausted", False
                self.metrics.counts["deadline_exhausted"] += 1
            outcome = Outcome(reason == "success", reason, progress.attempts, time.monotonic() - start, progress.response)
            if self.circuit and permit is not None:
                self.circuit.complete(permit, healthy=healthy)
            self.metrics.record(outcome)
            return outcome
        except asyncio.CancelledError:
            self.metrics.counts["cancelled"] += 1
            if self.circuit and permit is not None:
                self.circuit.abandon(permit)
            raise
        except Exception:
            # Programming errors in an injected adapter remain visible to callers.
            if self.circuit and permit is not None:
                self.circuit.abandon(permit)
            raise
        finally:
            self.active -= 1

    async def _execute(self, url: str, method: str, start: float, progress: _Progress) -> tuple[str, bool]:
        for attempt in range(1, self.policy.max_attempts + 1):
            remaining = self.policy.total_timeout - (time.monotonic() - start)
            if remaining <= 0:
                raise TimeoutError
            progress.attempts += 1
            progress.response = None
            self.metrics.counts["attempts"] += 1
            if attempt > 1:
                self.metrics.counts["retries"] += 1
            try:
                response = await asyncio.wait_for(self.transport(url, method), timeout=min(self.policy.attempt_timeout, remaining))
                progress.response = response
                if 200 <= response.status < 300:
                    return "success", True
                if response.status not in RETRYABLE:
                    return "non_retryable_status", response.status < 500
                server_delay = retry_after_seconds(response.headers.get("retry-after"))
                failure = "attempts_exhausted"
            except TransportError:
                self.metrics.counts["transport_errors"] += 1
                return "protocol_error", False
            except (TimeoutError, OSError):
                self.metrics.counts["transport_errors"] += 1
                server_delay = 0
                failure = "attempts_exhausted"
            if attempt == self.policy.max_attempts:
                return failure, False
            delay = max(server_delay, self.rng.uniform(0, min(self.policy.max_delay, self.policy.base_delay * 2 ** (attempt - 1))))
            if delay >= self.policy.total_timeout - (time.monotonic() - start):
                self.metrics.counts["deadline_exhausted"] += 1
                return "retry_would_exceed_deadline", False
            if self.budget is not None and not self.budget.take():
                self.metrics.counts["budget_rejected"] += 1
                return "retry_budget_exhausted", False
            # Reserving credit before sleeping is conservative: cancellation may
            # burn a token, but can never increase traffic beyond the budget.
            if delay:
                await asyncio.sleep(delay)
        raise AssertionError("max_attempts is validated as positive")

    def snapshot(self) -> dict:
        return {**self.metrics.snapshot(), "active": self.active, "retry_tokens": round(self.budget.tokens, 3) if self.budget is not None else None, "circuit": self.circuit.snapshot() if self.circuit else {"state": "disabled"}}
