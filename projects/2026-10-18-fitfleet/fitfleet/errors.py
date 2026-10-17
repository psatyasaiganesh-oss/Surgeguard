class FitFleetError(Exception):
    """Expected input, evidence, or operational failure."""


class ValidationError(FitFleetError):
    pass


class EvidenceError(FitFleetError):
    pass


class Conflict(FitFleetError):
    pass


class NotFound(FitFleetError):
    pass


class InvalidState(FitFleetError):
    pass


class Paused(FitFleetError):
    pass


class Busy(FitFleetError):
    pass
