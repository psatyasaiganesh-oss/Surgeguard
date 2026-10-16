"""Public errors; callers can retry Busy and lease acquisition separately."""


class MigrationError(Exception):
    """Base class for expected operational failures."""


class ValidationError(MigrationError):
    pass


class InvalidTransition(MigrationError):
    pass


class Conflict(MigrationError):
    pass


class NotFound(MigrationError):
    pass


class Busy(MigrationError):
    pass


class LeaseBusy(MigrationError):
    pass


class LeaseLost(MigrationError):
    pass


class StaleBatch(MigrationError):
    pass


class ConsistencyError(MigrationError):
    def __init__(self, report):
        self.report = report
        super().__init__(f"cutover blocked: {report['mismatches']} inconsistent customers")
