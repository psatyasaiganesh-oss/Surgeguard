class ClearFlowError(Exception):
    """Base class for expected domain failures."""


class ValidationError(ClearFlowError):
    pass


class AuthenticationError(ClearFlowError):
    pass


class NotFoundError(ClearFlowError):
    pass


class ConflictError(ClearFlowError):
    pass


class IdempotencyConflict(ConflictError):
    pass


class StateError(ConflictError):
    pass


class IntegrityError(ClearFlowError):
    pass
