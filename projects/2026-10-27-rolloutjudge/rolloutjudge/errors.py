"""Domain errors exposed by RolloutJudge."""


class RolloutJudgeError(Exception):
    code = "rolloutjudge_error"
    status = 500


class ValidationError(RolloutJudgeError):
    code = "validation_error"
    status = 400


class AuthorizationError(RolloutJudgeError):
    code = "unauthorized"
    status = 401


class NotFoundError(RolloutJudgeError):
    code = "not_found"
    status = 404


class ConflictError(RolloutJudgeError):
    code = "conflict"
    status = 409


class IdempotencyConflict(ConflictError):
    code = "idempotency_conflict"


class StateError(ConflictError):
    code = "invalid_state"


class IntegrityError(RolloutJudgeError):
    code = "integrity_error"
    status = 503
