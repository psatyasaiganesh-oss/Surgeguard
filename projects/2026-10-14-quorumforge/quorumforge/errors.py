"""Domain errors returned by the simulator and HTTP boundary."""


class QuorumError(Exception):
    """Base class for expected cluster errors."""


class ValidationError(QuorumError, ValueError):
    """Caller input or configuration is invalid."""


class UnavailableError(QuorumError):
    """Too few reachable replicas answered the requested operation."""


class ConflictError(QuorumError):
    """Versions with indistinguishable causality contain different data."""
