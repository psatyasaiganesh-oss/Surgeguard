"""Domain exceptions for ChronoCommit."""


class ChronoCommitError(Exception):
    """Base class for errors safe to expose as structured API responses."""

    code = "chronocommit_error"
    status = 500


class ValidationError(ChronoCommitError):
    code = "validation_error"
    status = 400


class AuthorizationError(ChronoCommitError):
    code = "unauthorized"
    status = 401


class NotFoundError(ChronoCommitError):
    code = "not_found"
    status = 404


class ConflictError(ChronoCommitError):
    code = "transaction_conflict"
    status = 409


class IdempotencyConflict(ConflictError):
    code = "idempotency_conflict"


class InsufficientFunds(ConflictError):
    code = "insufficient_funds"


class IntegrityError(ChronoCommitError):
    code = "integrity_error"
    status = 503


class ClockUnsafeError(ChronoCommitError):
    code = "clock_unsafe"
    status = 503


class TransactionAborted(ConflictError):
    code = "transaction_aborted"


class SimulatedCrash(RuntimeError):
    """Fault-injection exception used only by tests and the local demo."""

