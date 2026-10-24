"""Clock-uncertainty abstractions used to demonstrate commit wait."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol

from .errors import ClockUnsafeError, ValidationError
from .model import MAX_TIMESTAMP_NS, validate_timestamp


@dataclass(frozen=True)
class TimeInterval:
    earliest_ns: int
    latest_ns: int

    def __post_init__(self) -> None:
        validate_timestamp(self.earliest_ns, "earliest_ns")
        validate_timestamp(self.latest_ns, "latest_ns")
        if self.earliest_ns > self.latest_ns:
            raise ValidationError("earliest_ns cannot exceed latest_ns")

    @property
    def uncertainty_ns(self) -> int:
        return self.latest_ns - self.earliest_ns


class BoundedClock(Protocol):
    def now(self) -> TimeInterval: ...

    def wait_until_after(self, timestamp_ns: int, timeout_seconds: float = 5.0) -> int: ...


class SystemBoundedClock:
    """A local demonstration clock with a caller-supplied uncertainty bound.

    This is not TrueTime. It models the API shape and commit-wait rule while
    explicitly relying on the host clock and the configured uncertainty.
    """

    def __init__(
        self,
        uncertainty_ns: int = 100_000,
        *,
        wall_time_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if isinstance(uncertainty_ns, bool) or not isinstance(uncertainty_ns, int):
            raise ValidationError("uncertainty_ns must be an integer")
        if uncertainty_ns < 0 or uncertainty_ns > 1_000_000_000:
            raise ValidationError("uncertainty_ns must be between 0 and 1 second")
        self.uncertainty_ns = uncertainty_ns
        self._wall_time_ns = wall_time_ns
        self._sleep = sleep
        self._last_center = -1
        self._lock = threading.Lock()

    def now(self) -> TimeInterval:
        with self._lock:
            center = self._wall_time_ns()
            if center < self._last_center:
                raise ClockUnsafeError("host wall clock moved backwards")
            self._last_center = center
        earliest = max(0, center - self.uncertainty_ns)
        latest = min(MAX_TIMESTAMP_NS, center + self.uncertainty_ns)
        return TimeInterval(earliest, latest)

    def wait_until_after(self, timestamp_ns: int, timeout_seconds: float = 5.0) -> int:
        validate_timestamp(timestamp_ns)
        if timeout_seconds <= 0:
            raise ValidationError("timeout_seconds must be positive")
        deadline = time.monotonic() + timeout_seconds
        while True:
            interval = self.now()
            if interval.earliest_ns > timestamp_ns:
                return interval.earliest_ns
            if time.monotonic() >= deadline:
                raise ClockUnsafeError("commit wait exceeded its safety timeout")
            remaining_ns = timestamp_ns - interval.earliest_ns + 1
            self._sleep(min(max(remaining_ns / 1_000_000_000, 0.000_001), 0.01))


class ManualBoundedClock:
    """Deterministic clock for tests and reproducible demonstrations."""

    def __init__(self, center_ns: int = 1_000_000, uncertainty_ns: int = 10) -> None:
        validate_timestamp(center_ns, "center_ns")
        if uncertainty_ns < 0 or uncertainty_ns > center_ns:
            raise ValidationError("uncertainty_ns must be non-negative and <= center_ns")
        self.center_ns = center_ns
        self.uncertainty_ns = uncertainty_ns
        self.wait_count = 0
        self.waited_ns = 0
        self._lock = threading.Lock()

    def now(self) -> TimeInterval:
        with self._lock:
            return TimeInterval(
                self.center_ns - self.uncertainty_ns,
                min(MAX_TIMESTAMP_NS, self.center_ns + self.uncertainty_ns),
            )

    def advance(self, nanoseconds: int) -> None:
        if isinstance(nanoseconds, bool) or not isinstance(nanoseconds, int) or nanoseconds < 0:
            raise ValidationError("advance must be a non-negative integer")
        with self._lock:
            if self.center_ns + nanoseconds > MAX_TIMESTAMP_NS:
                raise ClockUnsafeError("manual clock overflow")
            self.center_ns += nanoseconds

    def set_center(self, center_ns: int) -> None:
        validate_timestamp(center_ns, "center_ns")
        if center_ns < self.uncertainty_ns:
            raise ValidationError("center_ns is smaller than uncertainty")
        with self._lock:
            self.center_ns = center_ns

    def wait_until_after(self, timestamp_ns: int, timeout_seconds: float = 5.0) -> int:
        del timeout_seconds
        validate_timestamp(timestamp_ns)
        with self._lock:
            earliest = self.center_ns - self.uncertainty_ns
            delta = max(0, timestamp_ns - earliest + 1)
            if self.center_ns + delta > MAX_TIMESTAMP_NS:
                raise ClockUnsafeError("manual clock overflow during commit wait")
            self.center_ns += delta
            self.wait_count += 1
            self.waited_ns += delta
            return self.center_ns - self.uncertainty_ns

