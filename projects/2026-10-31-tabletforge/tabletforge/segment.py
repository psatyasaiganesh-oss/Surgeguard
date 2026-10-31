from __future__ import annotations

import bisect
import hashlib
import os
import struct
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .bloom import BloomFilter
from .errors import IntegrityError, ValidationError
from .model import (
    VersionedRecord,
    canonical_json,
    parse_json,
    require_closed_object,
    require_hex_digest,
    require_int,
    require_key,
)


MAGIC = b"TFSST01\n"
TRAILER_MAGIC = b"TFEND01\n"
MAX_RECORD_BYTES = 2_097_152
MAX_FOOTER_BYTES = 16_777_216


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1_048_576):
            hasher.update(chunk)
    return hasher.hexdigest()


@dataclass(frozen=True)
class SegmentMetadata:
    segment_id: int
    filename: str
    sha256: str
    min_key: str
    max_key: str
    min_sequence: int
    max_sequence: int
    record_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "segment_id": self.segment_id,
            "filename": self.filename,
            "sha256": self.sha256,
            "min_key": self.min_key,
            "max_key": self.max_key,
            "min_sequence": self.min_sequence,
            "max_sequence": self.max_sequence,
            "record_count": self.record_count,
        }

    @classmethod
    def from_value(cls, value: Any) -> "SegmentMetadata":
        body = require_closed_object(
            value,
            "segment metadata",
            {
                "segment_id",
                "filename",
                "sha256",
                "min_key",
                "max_key",
                "min_sequence",
                "max_sequence",
                "record_count",
            },
        )
        segment_id = require_int(body["segment_id"], "segment_id", minimum=1)
        filename = body["filename"]
        expected = f"segment-{segment_id:08d}.sst"
        if filename != expected:
            raise ValidationError("segment filename does not match its identity")
        minimum_key = require_key(body["min_key"], "min_key")
        maximum_key = require_key(body["max_key"], "max_key")
        if minimum_key > maximum_key:
            raise ValidationError("segment key range is invalid")
        minimum_sequence = require_int(body["min_sequence"], "min_sequence", minimum=1)
        maximum_sequence = require_int(body["max_sequence"], "max_sequence", minimum=minimum_sequence)
        return cls(
            segment_id,
            filename,
            require_hex_digest(body["sha256"], "segment sha256"),
            minimum_key,
            maximum_key,
            minimum_sequence,
            maximum_sequence,
            require_int(body["record_count"], "record_count", minimum=1),
        )


def write_segment(
    directory: Path, segment_id: int, records: list[VersionedRecord]
) -> SegmentMetadata:
    if not records:
        raise ValidationError("cannot write an empty segment")
    ordered = sorted(records, key=lambda item: (item.key, -item.sequence))
    identities = [(item.key, item.sequence) for item in ordered]
    if len(identities) != len(set(identities)):
        raise ValidationError("segment records contain duplicate key/sequence pairs")
    filename = f"segment-{segment_id:08d}.sst"
    final_path = directory / filename
    if final_path.exists():
        raise IntegrityError("segment identity already exists")
    temporary = directory / f".{filename}.{uuid.uuid4().hex}.tmp"
    index: list[list[Any]] = []
    data_hasher = hashlib.sha256()
    try:
        with temporary.open("xb") as stream:
            stream.write(MAGIC)
            data_hasher.update(MAGIC)
            previous_key: str | None = None
            for record in ordered:
                offset = stream.tell()
                if record.key != previous_key:
                    index.append([record.key, offset])
                    previous_key = record.key
                payload = canonical_json(record.as_dict()).encode()
                if len(payload) > MAX_RECORD_BYTES:
                    raise ValidationError("segment record is too large")
                frame = struct.pack(">I", len(payload)) + payload
                stream.write(frame)
                data_hasher.update(frame)
            data_end = stream.tell()
            footer = {
                "version": 1,
                "segment_id": segment_id,
                "record_count": len(ordered),
                "min_key": ordered[0].key,
                "max_key": ordered[-1].key,
                "min_sequence": min(item.sequence for item in ordered),
                "max_sequence": max(item.sequence for item in ordered),
                "data_end": data_end,
                "data_sha256": data_hasher.hexdigest(),
                "index": index,
                "bloom": BloomFilter.build(item.key for item in ordered).as_dict(),
            }
            footer_bytes = canonical_json(footer).encode()
            if len(footer_bytes) > MAX_FOOTER_BYTES:
                raise ValidationError("segment footer is too large")
            stream.write(footer_bytes)
            stream.write(struct.pack(">Q", len(footer_bytes)))
            stream.write(TRAILER_MAGIC)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, final_path)
        _fsync_directory(directory)
        segment_digest = file_sha256(final_path)
        return SegmentMetadata(
            segment_id,
            filename,
            segment_digest,
            ordered[0].key,
            ordered[-1].key,
            min(item.sequence for item in ordered),
            max(item.sequence for item in ordered),
            len(ordered),
        )
    finally:
        if temporary.exists():
            temporary.unlink()


class SegmentReader:
    def __init__(self, path: Path, metadata: SegmentMetadata) -> None:
        self.path = path
        self.metadata = metadata
        if not path.is_file():
            raise IntegrityError(f"referenced segment is missing: {metadata.filename}")
        if file_sha256(path) != metadata.sha256:
            raise IntegrityError(f"segment digest mismatch: {metadata.filename}")
        self._footer = self._read_footer()
        self._bloom = BloomFilter.from_value(self._footer["bloom"])
        self._keys = [item[0] for item in self._footer["index"]]
        self._offsets = [item[1] for item in self._footer["index"]]

    @staticmethod
    def _read_exact(stream, count: int) -> bytes:
        value = stream.read(count)
        if len(value) != count:
            raise IntegrityError("segment is truncated")
        return value

    def _read_footer(self) -> dict[str, Any]:
        size = self.path.stat().st_size
        if size < len(MAGIC) + 16:
            raise IntegrityError("segment is too short")
        with self.path.open("rb") as stream:
            if self._read_exact(stream, len(MAGIC)) != MAGIC:
                raise IntegrityError("segment header is invalid")
            stream.seek(-16, os.SEEK_END)
            footer_length = struct.unpack(">Q", self._read_exact(stream, 8))[0]
            if self._read_exact(stream, 8) != TRAILER_MAGIC:
                raise IntegrityError("segment trailer is invalid")
            if not 1 <= footer_length <= MAX_FOOTER_BYTES:
                raise IntegrityError("segment footer length is invalid")
            footer_start = size - 16 - footer_length
            if footer_start <= len(MAGIC):
                raise IntegrityError("segment footer offset is invalid")
            stream.seek(footer_start)
            raw = self._read_exact(stream, footer_length)
            try:
                footer = parse_json(raw.decode("utf-8"), "segment footer")
            except UnicodeDecodeError as exc:
                raise IntegrityError("segment footer is not UTF-8") from exc
            try:
                footer = require_closed_object(
                    footer,
                    "segment footer",
                    {
                        "version",
                        "segment_id",
                        "record_count",
                        "min_key",
                        "max_key",
                        "min_sequence",
                        "max_sequence",
                        "data_end",
                        "data_sha256",
                        "index",
                        "bloom",
                    },
                )
                if require_int(footer["version"], "version", minimum=1, maximum=1) != 1:
                    raise ValidationError("unsupported segment version")
                if require_int(footer["segment_id"], "segment_id", minimum=1) != self.metadata.segment_id:
                    raise ValidationError("segment identity mismatch")
                if require_int(footer["record_count"], "record_count", minimum=1) != self.metadata.record_count:
                    raise ValidationError("segment record count mismatch")
                if footer["min_key"] != self.metadata.min_key or footer["max_key"] != self.metadata.max_key:
                    raise ValidationError("segment key range mismatch")
                if footer["min_sequence"] != self.metadata.min_sequence or footer["max_sequence"] != self.metadata.max_sequence:
                    raise ValidationError("segment sequence range mismatch")
                data_end = require_int(footer["data_end"], "data_end", minimum=len(MAGIC) + 1)
                if data_end != footer_start:
                    raise ValidationError("segment data boundary is invalid")
                require_hex_digest(footer["data_sha256"], "data_sha256")
                raw_index = footer["index"]
                if not isinstance(raw_index, list) or not raw_index:
                    raise ValidationError("segment index must be non-empty")
                last_key: str | None = None
                last_offset = len(MAGIC) - 1
                for item in raw_index:
                    if not isinstance(item, list) or len(item) != 2:
                        raise ValidationError("segment index entry is invalid")
                    key = require_key(item[0], "index key")
                    offset = require_int(item[1], "index offset", minimum=len(MAGIC), maximum=data_end - 1)
                    if last_key is not None and key <= last_key:
                        raise ValidationError("segment index keys are not strictly ordered")
                    if offset <= last_offset:
                        raise ValidationError("segment index offsets are not strictly ordered")
                    last_key, last_offset = key, offset
                BloomFilter.from_value(footer["bloom"])
            except ValidationError as exc:
                raise IntegrityError(str(exc)) from exc
            stream.seek(0)
            hasher = hashlib.sha256()
            remaining = footer_start
            while remaining:
                chunk = stream.read(min(1_048_576, remaining))
                if not chunk:
                    raise IntegrityError("segment data is truncated")
                hasher.update(chunk)
                remaining -= len(chunk)
            if hasher.hexdigest() != footer["data_sha256"]:
                raise IntegrityError("segment data digest is invalid")
        return footer

    def might_contain(self, key: str) -> bool:
        return self.metadata.min_key <= key <= self.metadata.max_key and self._bloom.might_contain(key)

    def _read_record(self, stream) -> VersionedRecord:
        header = stream.read(4)
        if len(header) != 4:
            raise IntegrityError("segment record header is truncated")
        length = struct.unpack(">I", header)[0]
        if not 1 <= length <= MAX_RECORD_BYTES:
            raise IntegrityError("segment record length is invalid")
        raw = self._read_exact(stream, length)
        try:
            value = parse_json(raw.decode("utf-8"), "segment record")
            return VersionedRecord.from_value(value)
        except (UnicodeDecodeError, ValidationError) as exc:
            raise IntegrityError("segment record is invalid") from exc

    def get(self, key: str, sequence: int) -> VersionedRecord | None:
        position = bisect.bisect_left(self._keys, key)
        if position >= len(self._keys) or self._keys[position] != key:
            return None
        with self.path.open("rb") as stream:
            stream.seek(self._offsets[position])
            while stream.tell() < self._footer["data_end"]:
                record = self._read_record(stream)
                if record.key != key:
                    break
                if record.sequence <= sequence:
                    return record
        return None

    def records(self) -> Iterator[VersionedRecord]:
        with self.path.open("rb") as stream:
            stream.seek(len(MAGIC))
            while stream.tell() < self._footer["data_end"]:
                yield self._read_record(stream)
