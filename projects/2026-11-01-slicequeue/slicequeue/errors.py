class SliceQueueError(Exception):
    """Base class for expected domain failures."""


class ValidationError(SliceQueueError):
    pass


class ConflictError(SliceQueueError):
    pass


class NotFoundError(SliceQueueError):
    pass


class LeaseError(SliceQueueError):
    pass


class IntegrityError(SliceQueueError):
    pass


class StateError(SliceQueueError):
    pass


class SimulatedCrash(SliceQueueError):
    """Deterministic failure after a durable transaction commits."""
