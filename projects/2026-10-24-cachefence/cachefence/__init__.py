"""CacheFence: race-safe cache invalidation and consistency observability."""

from .cache import ApplyResult, CacheStore, LookupResult
from .model import ConflictError, IntegrityError, ValidationError
from .origin import OriginStore
from .service import CacheAsideService, ReadResult

__all__ = [
    "CacheAsideService",
    "ApplyResult",
    "CacheStore",
    "LookupResult",
    "ConflictError",
    "IntegrityError",
    "OriginStore",
    "ReadResult",
    "ValidationError",
]
__version__ = "1.0.0"
