class ExitPackError(Exception):
    """Base class for expected domain failures."""


class ValidationError(ExitPackError):
    pass


class ConflictError(ExitPackError):
    pass


class NotFoundError(ExitPackError):
    pass


class CapacityError(ExitPackError):
    pass


class IntegrityError(ExitPackError):
    pass


class AuthorizationError(ExitPackError):
    pass


class SimulatedCrash(ExitPackError):
    """A deterministic failure raised after a durable transaction commits."""
