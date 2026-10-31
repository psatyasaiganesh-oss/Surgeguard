from __future__ import annotations

import hashlib
import hmac
import json
import os
import struct
import uuid
import zlib
from pathlib import Path
from typing import Any

from .errors import IntegrityError, ValidationError
from .model import canonical_json, parse_json


MAX_FRAME_BYTES = 4_194_304
TRAILER_BYTES = 4 + 32


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class WriteAheadLog:
    def __init__(self, path: Path, secret: bytes) -> None:
        self.path = path
        self.secret = secret
        path.touch(exist_ok=True)

    def append(self, value: dict[str, Any]) -> int:
        payload = canonical_json(value).encode()
        if not 1 <= len(payload) <= MAX_FRAME_BYTES:
            raise ValidationError("WAL frame has an invalid size")
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        signature = hmac.new(self.secret, payload, hashlib.sha256).digest()
        frame = struct.pack(">I", len(payload)) + payload + struct.pack(">I", checksum) + signature
        with self.path.open("ab") as stream:
            stream.write(frame)
            stream.flush()
            os.fsync(stream.fileno())
            return stream.tell()

    def recover(self) -> tuple[list[dict[str, Any]], int]:
        frames: list[dict[str, Any]] = []
        last_good = 0
        size = self.path.stat().st_size
        with self.path.open("r+b") as stream:
            while stream.tell() < size:
                frame_start = stream.tell()
                header = stream.read(4)
                if len(header) < 4:
                    stream.truncate(last_good)
                    stream.flush()
                    os.fsync(stream.fileno())
                    return frames, size - last_good
                length = struct.unpack(">I", header)[0]
                if not 1 <= length <= MAX_FRAME_BYTES:
                    raise IntegrityError(f"WAL frame length is invalid at offset {frame_start}")
                payload = stream.read(length)
                trailer = stream.read(TRAILER_BYTES)
                if len(payload) < length or len(trailer) < TRAILER_BYTES:
                    stream.truncate(last_good)
                    stream.flush()
                    os.fsync(stream.fileno())
                    return frames, size - last_good
                expected_crc = struct.unpack(">I", trailer[:4])[0]
                if zlib.crc32(payload) & 0xFFFFFFFF != expected_crc:
                    raise IntegrityError(f"WAL checksum mismatch at offset {frame_start}")
                expected_hmac = hmac.new(self.secret, payload, hashlib.sha256).digest()
                if not hmac.compare_digest(trailer[4:], expected_hmac):
                    raise IntegrityError(f"WAL signature mismatch at offset {frame_start}")
                try:
                    decoded = payload.decode("utf-8")
                    value = parse_json(decoded, "WAL frame")
                except (UnicodeDecodeError, ValidationError) as exc:
                    raise IntegrityError(f"WAL payload is invalid at offset {frame_start}") from exc
                if not isinstance(value, dict):
                    raise IntegrityError("WAL payload must be an object")
                frames.append(value)
                last_good = stream.tell()
        return frames, 0

    def reset(self) -> None:
        temporary = self.path.parent / f".{self.path.name}.{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            _fsync_directory(self.path.parent)
        finally:
            if temporary.exists():
                temporary.unlink()
