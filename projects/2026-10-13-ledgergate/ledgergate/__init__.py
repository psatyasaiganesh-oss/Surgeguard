"""LedgerGate: durable, deduplicated processing for signed payment webhooks."""

from .security import SignatureError, sign_payload, verify_signature
from .service import LedgerGate, Receipt
from .store import SQLiteStore

__all__ = [
    "LedgerGate",
    "Receipt",
    "SQLiteStore",
    "SignatureError",
    "sign_payload",
    "verify_signature",
]
__version__ = "1.0.0"
