"""SurgeGuard: a bounded, observable retry client for read-only HTTP calls."""

from .client import Outcome, Policy, ResilientClient
from .controls import CircuitBreaker, RetryBudget
from .transport import HttpTransport, Response

__all__ = ["CircuitBreaker", "HttpTransport", "Outcome", "Policy", "ResilientClient", "Response", "RetryBudget"]
__version__ = "1.0.0"
