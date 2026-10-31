class TabletForgeError(Exception):
    """Base class for expected domain failures."""


class ValidationError(TabletForgeError):
    pass


class ConflictError(TabletForgeError):
    pass


class NotFoundError(TabletForgeError):
    pass


class IntegrityError(TabletForgeError):
    pass


class StateError(TabletForgeError):
    pass


class SimulatedCrash(TabletForgeError):
    """Deterministic failpoint used to prove recovery ordering."""
