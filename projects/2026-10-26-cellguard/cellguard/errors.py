"""Domain errors for CellGuard."""


class CellGuardError(Exception):
    code = "cellguard_error"
    status = 500


class ValidationError(CellGuardError):
    code = "validation_error"
    status = 400


class AuthorizationError(CellGuardError):
    code = "unauthorized"
    status = 401


class NotFoundError(CellGuardError):
    code = "not_found"
    status = 404


class ConflictError(CellGuardError):
    code = "conflict"
    status = 409


class IdempotencyConflict(ConflictError):
    code = "idempotency_conflict"


class CapacityError(CellGuardError):
    code = "capacity_exhausted"
    status = 429


class UnavailableError(CellGuardError):
    code = "assigned_cells_unavailable"
    status = 503


class IntegrityError(CellGuardError):
    code = "integrity_error"
    status = 503


class LeaseError(ConflictError):
    code = "lease_error"
