"""MergeMesh: conflict-visible, multi-region document replication."""

from .model import ConflictError, IntegrityError, ValidationError
from .store import RegionStore

__all__ = ["ConflictError", "IntegrityError", "RegionStore", "ValidationError"]
__version__ = "1.0.0"
