"""Cache-aside read service with per-key request coalescing."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable

from .cache import CacheStore
from .model import Snapshot, ValidationError, validate_id, validate_ns
from .origin import OriginStore


@dataclass(frozen=True)
class ReadResult:
    status: str
    source: str
    snapshot: Snapshot | None
    stale_fill_retried: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "source": self.source,
            "stale_fill_retried": self.stale_fill_retried,
            "snapshot": None if self.snapshot is None else self.snapshot.as_dict(),
        }


class CacheAsideService:
    def __init__(self, origin: OriginStore, cache: CacheStore) -> None:
        self.origin = origin
        self.cache = cache
        self._locks: dict[tuple[str, str], threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock_for(self, tenant: str, key: str) -> threading.Lock:
        identity = (tenant, key)
        with self._locks_guard:
            return self._locks.setdefault(identity, threading.Lock())

    def get(
        self,
        tenant: str,
        key: str,
        *,
        ttl_ns: int = 60_000_000_000,
        now_ns: int | None = None,
        source_delay_s: float = 0.0,
        after_origin_read: Callable[[Snapshot], None] | None = None,
    ) -> ReadResult:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        if isinstance(ttl_ns, bool) or not isinstance(ttl_ns, int) or ttl_ns <= 0:
            raise ValidationError("ttl_ns must be a positive integer")
        if not isinstance(source_delay_s, (int, float)) or source_delay_s < 0 or source_delay_s > 5:
            raise ValidationError("source_delay_s must be between 0 and 5 seconds")

        cached = self.cache.lookup(tenant, key, now_ns=now)
        if cached.status != "miss":
            return ReadResult(
                status="not_found" if cached.status == "negative" else "ok",
                source="cache",
                snapshot=cached.snapshot,
            )

        with self._lock_for(tenant, key):
            cached = self.cache.lookup(tenant, key, now_ns=now)
            if cached.status != "miss":
                return ReadResult(
                    status="not_found" if cached.status == "negative" else "ok",
                    source="cache_after_wait",
                    snapshot=cached.snapshot,
                )
            snapshot = self.origin.read_snapshot(tenant, key)
            if snapshot is None:
                return ReadResult(status="not_found", source="origin", snapshot=None)
            if after_origin_read is not None:
                after_origin_read(snapshot)
            if source_delay_s:
                time.sleep(source_delay_s)
            installed = self.cache.install_fill(snapshot, ttl_ns=ttl_ns, now_ns=now)
            retried = installed.status == "stale"
            if retried:
                snapshot = self.origin.read_snapshot(tenant, key)
                if snapshot is None:
                    return ReadResult(
                        status="not_found", source="origin_retry", snapshot=None, stale_fill_retried=True
                    )
                second = self.cache.install_fill(snapshot, ttl_ns=ttl_ns, now_ns=now)
                if second.status != "installed":
                    raise RuntimeError("origin did not advance beyond the cache fence")
            return ReadResult(
                status="not_found" if snapshot.deleted else "ok",
                source="origin_retry" if retried else "origin",
                snapshot=snapshot,
                stale_fill_retried=retried,
            )
