"""Content-addressed result storage with atomic publication and verification."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from pathlib import Path

from .model import MAX_REQUEST_BYTES, ValidationError


class IntegrityError(RuntimeError):
    """A content-addressed object does not match its recorded digest."""


class BlobStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.name != "nt":
            os.chmod(self.root, 0o700)

    def _path(self, digest: str) -> Path:
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ValidationError("invalid blob digest")
        return self.root / digest[:2] / digest[2:]

    @staticmethod
    def hash(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def put(self, data: bytes) -> str:
        if not isinstance(data, bytes):
            raise ValidationError("output must be bytes")
        if not 1 <= len(data) <= MAX_REQUEST_BYTES:
            raise ValidationError("output size must be in [1, 1048576] bytes")
        digest = self.hash(data)
        target = self._path(digest)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if target.exists():
            if self.get(digest) != data:
                raise IntegrityError("existing content-addressed blob is corrupt")
            return digest
        temporary = target.parent / f".{target.name}.{secrets.token_hex(8)}.tmp"
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()
        return digest

    def get(self, digest: str) -> bytes:
        path = self._path(digest)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise IntegrityError(f"missing blob: {digest}") from exc
        if not hmac.compare_digest(self.hash(data), digest):
            raise IntegrityError(f"blob checksum mismatch: {digest}")
        return data

    def verify(self, digest: str) -> bool:
        self.get(digest)
        return True
