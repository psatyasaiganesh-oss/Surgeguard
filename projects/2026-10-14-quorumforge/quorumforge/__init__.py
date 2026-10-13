"""QuorumForge: a deterministic, failure-aware replicated key-value lab."""

from .clock import ClockRelation, VectorClock
from .cluster import ClusterConfig, QuorumCluster, ReadResult, WriteResult
from .errors import ConflictError, QuorumError, UnavailableError, ValidationError

__all__ = [
    "ClockRelation",
    "ClusterConfig",
    "ConflictError",
    "QuorumCluster",
    "QuorumError",
    "ReadResult",
    "UnavailableError",
    "ValidationError",
    "VectorClock",
    "WriteResult",
]

__version__ = "1.0.0"
