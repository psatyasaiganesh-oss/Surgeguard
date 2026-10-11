"""Small, event-loop-confined resilience controls with injectable clocks."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable


def positive_number(name: str, value: float, *, allow_zero: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    if value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{name} must be {'nonnegative' if allow_zero else 'positive'}")


class RetryBudget:
    """A retry-only token bucket. First attempts never spend a retry token.

    For any interval of duration T, at most capacity + refill_rate * T
    retries can be admitted. One instance belongs to one dependency/client.
    Methods must be called from the owning asyncio event loop.
    """

    def __init__(self, capacity: float = 10, refill_rate: float = 1, *, clock: Callable[[], float] = time.monotonic):
        positive_number("capacity", capacity, allow_zero=True)
        positive_number("refill_rate", refill_rate, allow_zero=True)
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._tokens = float(capacity)
        self._clock = clock
        self._updated = clock()

    def take(self) -> bool:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._updated = max(self._updated, now)
        if self._tokens < 1:
            return False
        self._tokens -= 1
        return True

    @property
    def tokens(self) -> float:
        """Stored balance; refill occurs on the next take()."""
        return self._tokens


@dataclass(frozen=True)
class Permit:
    generation: int
    probe: bool = False


class CircuitBreaker:
    """Optional consecutive-failure breaker with one half-open probe.

    Epochs prevent a late completion from an old request from closing a
    newly opened circuit. Cancellation frees the half-open probe slot.
    Counts logical requests, not each retry within a request.
    """

    def __init__(self, threshold: int = 5, cooldown: float = 1, *, clock: Callable[[], float] = time.monotonic):
        if type(threshold) is not int or threshold < 1:
            raise ValueError("threshold must be a positive integer")
        positive_number("cooldown", cooldown)
        self.threshold = threshold
        self.cooldown = cooldown
        self._clock = clock
        self.state = "closed"
        self.failures = 0
        self._generation = 0
        self._open_until = 0.0

    def acquire(self) -> Permit | None:
        if self.state == "closed":
            return Permit(self._generation)
        if self.state == "open" and self._clock() >= self._open_until:
            self.state = "half_open"
            return Permit(self._generation, probe=True)
        return None

    def complete(self, permit: Permit, *, healthy: bool) -> None:
        if permit.generation != self._generation:
            return
        if permit.probe:
            if self.state != "half_open":
                return
            self._generation += 1
            self.failures = 0
            if healthy:
                self.state = "closed"
            else:
                self.state = "open"
                self._open_until = self._clock() + self.cooldown
            return
        if self.state != "closed":
            return
        self.failures = 0 if healthy else self.failures + 1
        if self.failures >= self.threshold:
            self.state = "open"
            self._generation += 1
            self._open_until = self._clock() + self.cooldown

    def abandon(self, permit: Permit) -> None:
        if permit.probe and permit.generation == self._generation and self.state == "half_open":
            self.state = "open"
            self._open_until = self._clock()

    def snapshot(self) -> dict:
        return {"state": self.state, "consecutive_failures": self.failures, "generation": self._generation}
