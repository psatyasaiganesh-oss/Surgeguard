class TestSieveError(Exception):
    """Base class for expected TestSieve failures."""


class ValidationError(TestSieveError):
    pass


class AuthenticationError(TestSieveError):
    pass


class NotFoundError(TestSieveError):
    pass


class ConflictError(TestSieveError):
    pass


class IdempotencyConflict(ConflictError):
    pass


class StateError(ConflictError):
    pass


class IntegrityError(TestSieveError):
    pass
