from __future__ import annotations

import math
import os
import tempfile
import unittest
from pathlib import Path

from tabletforge.bloom import BloomFilter
from tabletforge.errors import IntegrityError, ValidationError
from tabletforge.model import (
    VersionedRecord,
    canonical_json,
    normalize_mutations,
    parse_json,
    require_closed_object,
    require_identifier,
    require_int,
    require_key,
    require_value,
)
from tabletforge.segment import SegmentMetadata, SegmentReader, write_segment
from tabletforge.wal import WriteAheadLog


SECRET = b"model-segment-unit-test-secret-more-than-32-bytes"


class ModelTests(unittest.TestCase):
    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"z": 1, "a": 2}), '{"a":2,"z":1}')

    def test_canonical_json_rejects_nan(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json({"bad": math.nan})

    def test_parse_json_rejects_duplicate_fields(self) -> None:
        with self.assertRaises(ValidationError):
            parse_json('{"a":1,"a":2}')

    def test_parse_json_rejects_nonfinite_numbers(self) -> None:
        with self.assertRaises(ValidationError):
            parse_json('{"bad":Infinity}')

    def test_closed_object_rejects_missing_field(self) -> None:
        with self.assertRaises(ValidationError):
            require_closed_object({}, "body", {"key"})

    def test_closed_object_rejects_unknown_field(self) -> None:
        with self.assertRaises(ValidationError):
            require_closed_object({"key": "x", "extra": 1}, "body", {"key"})

    def test_identifier_accepts_idempotency_key(self) -> None:
        self.assertEqual(require_identifier("load:0001", "key"), "load:0001")

    def test_identifier_rejects_slash(self) -> None:
        with self.assertRaises(ValidationError):
            require_identifier("bad/key", "key")

    def test_integer_rejects_boolean(self) -> None:
        with self.assertRaises(ValidationError):
            require_int(True, "count")

    def test_integer_enforces_range(self) -> None:
        with self.assertRaises(ValidationError):
            require_int(11, "count", maximum=10)

    def test_key_accepts_unicode(self) -> None:
        self.assertEqual(require_key("customer:గణేష్"), "customer:గణేష్")

    def test_key_rejects_nul(self) -> None:
        with self.assertRaises(ValidationError):
            require_key("bad\x00key")

    def test_key_size_is_bounded(self) -> None:
        with self.assertRaises(ValidationError):
            require_key("x" * 513)

    def test_value_must_be_string(self) -> None:
        with self.assertRaises(ValidationError):
            require_value(123)

    def test_put_record_round_trip(self) -> None:
        record = VersionedRecord.from_value(
            {"key": "a", "sequence": 1, "value": "v", "tombstone": False}
        )
        self.assertEqual(record.as_dict()["value"], "v")

    def test_tombstone_requires_null_value(self) -> None:
        with self.assertRaises(ValidationError):
            VersionedRecord.from_value(
                {"key": "a", "sequence": 1, "value": "v", "tombstone": True}
            )

    def test_mutations_normalize_put_and_delete(self) -> None:
        value = normalize_mutations(
            [
                {"operation": "PUT", "key": "a", "value": "1"},
                {"operation": "DELETE", "key": "b"},
            ]
        )
        self.assertEqual([item["operation"] for item in value], ["PUT", "DELETE"])

    def test_put_requires_value(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_mutations([{"operation": "PUT", "key": "a"}])

    def test_delete_rejects_value(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_mutations([{"operation": "DELETE", "key": "a", "value": "x"}])

    def test_batch_size_is_bounded(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_mutations(
                [{"operation": "DELETE", "key": f"k{i}"} for i in range(101)]
            )


class BloomTests(unittest.TestCase):
    def test_inserted_keys_have_no_false_negatives(self) -> None:
        keys = [f"key:{index}" for index in range(100)]
        bloom = BloomFilter.build(keys)
        self.assertTrue(all(bloom.might_contain(key) for key in keys))

    def test_bloom_round_trip(self) -> None:
        bloom = BloomFilter.build(["alpha", "beta"])
        restored = BloomFilter.from_value(bloom.as_dict())
        self.assertTrue(restored.might_contain("alpha"))
        self.assertEqual(restored.bits, bloom.bits)

    def test_invalid_base64_is_rejected(self) -> None:
        value = BloomFilter.build(["alpha"]).as_dict()
        value["bits_base64"] = "!!!"
        with self.assertRaises(ValidationError):
            BloomFilter.from_value(value)

    def test_wrong_bit_payload_length_is_rejected(self) -> None:
        value = BloomFilter.build(["alpha"]).as_dict()
        value["bit_count"] += 8
        with self.assertRaises(ValidationError):
            BloomFilter.from_value(value)


class SegmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.records = [
            VersionedRecord("a", 3, "new", False),
            VersionedRecord("a", 1, "old", False),
            VersionedRecord("c", 2, None, True),
        ]

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_write_and_read_segment(self) -> None:
        metadata = write_segment(self.directory, 1, self.records)
        reader = SegmentReader(self.directory / metadata.filename, metadata)
        self.assertEqual(reader.get("a", 3).value, "new")
        self.assertEqual(reader.get("a", 2).value, "old")

    def test_segment_records_are_key_then_sequence_ordered(self) -> None:
        metadata = write_segment(self.directory, 1, list(reversed(self.records)))
        reader = SegmentReader(self.directory / metadata.filename, metadata)
        self.assertEqual(
            [(item.key, item.sequence) for item in reader.records()],
            [("a", 3), ("a", 1), ("c", 2)],
        )

    def test_segment_bloom_rejects_definite_absence(self) -> None:
        metadata = write_segment(self.directory, 1, self.records)
        reader = SegmentReader(self.directory / metadata.filename, metadata)
        candidates = [f"b:{index}" for index in range(100)]
        self.assertTrue(any(not reader.might_contain(key) for key in candidates))

    def test_empty_segment_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            write_segment(self.directory, 1, [])

    def test_duplicate_record_identity_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            write_segment(self.directory, 1, [self.records[0], self.records[0]])

    def test_segment_identity_cannot_be_reused(self) -> None:
        write_segment(self.directory, 1, self.records)
        with self.assertRaises(IntegrityError):
            write_segment(self.directory, 1, self.records)

    def test_segment_tamper_is_detected(self) -> None:
        metadata = write_segment(self.directory, 1, self.records)
        path = self.directory / metadata.filename
        with path.open("r+b") as stream:
            stream.seek(12)
            byte = stream.read(1)
            stream.seek(12)
            stream.write(bytes([byte[0] ^ 1]))
        with self.assertRaises(IntegrityError):
            SegmentReader(path, metadata)

    def test_missing_segment_is_detected(self) -> None:
        metadata = write_segment(self.directory, 1, self.records)
        (self.directory / metadata.filename).unlink()
        with self.assertRaises(IntegrityError):
            SegmentReader(self.directory / metadata.filename, metadata)

    def test_metadata_filename_is_bound_to_identity(self) -> None:
        metadata = write_segment(self.directory, 1, self.records).as_dict()
        metadata["filename"] = "segment-00000002.sst"
        with self.assertRaises(ValidationError):
            SegmentMetadata.from_value(metadata)


class WalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "wal.log"
        self.wal = WriteAheadLog(self.path, SECRET)
        self.frame = {"version": 1, "message": "durable"}

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_append_and_recover(self) -> None:
        self.wal.append(self.frame)
        frames, truncated = self.wal.recover()
        self.assertEqual(frames, [self.frame])
        self.assertEqual(truncated, 0)

    def test_incomplete_tail_is_truncated(self) -> None:
        self.wal.append(self.frame)
        good_size = self.path.stat().st_size
        with self.path.open("ab") as stream:
            stream.write(b"\x00\x00")
        frames, truncated = self.wal.recover()
        self.assertEqual(frames, [self.frame])
        self.assertEqual(truncated, 2)
        self.assertEqual(self.path.stat().st_size, good_size)

    def test_checksum_corruption_is_detected(self) -> None:
        self.wal.append(self.frame)
        with self.path.open("r+b") as stream:
            stream.seek(8)
            byte = stream.read(1)
            stream.seek(8)
            stream.write(bytes([byte[0] ^ 1]))
        with self.assertRaises(IntegrityError):
            self.wal.recover()

    def test_signature_corruption_is_detected(self) -> None:
        self.wal.append(self.frame)
        with self.path.open("r+b") as stream:
            stream.seek(-1, os.SEEK_END)
            byte = stream.read(1)
            stream.seek(-1, os.SEEK_END)
            stream.write(bytes([byte[0] ^ 1]))
        with self.assertRaises(IntegrityError):
            self.wal.recover()

    def test_reset_replaces_log_with_empty_file(self) -> None:
        self.wal.append(self.frame)
        self.wal.reset()
        self.assertEqual(self.path.stat().st_size, 0)
        self.assertEqual(self.wal.recover(), ([], 0))


if __name__ == "__main__":
    unittest.main()
