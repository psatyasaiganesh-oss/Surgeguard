import copy
import json
import math
import unittest

from specuflow.model import (
    MAX_SHARDS,
    ValidationError,
    parse_counters,
    parse_job,
    parse_output,
    strict_json,
)
from specuflow.runner import make_job


class ModelTests(unittest.TestCase):
    def test_valid_job_is_normalized(self):
        parsed = parse_job(make_job("valid", shards=2))
        self.assertEqual(parsed["job_id"], "valid")
        self.assertEqual(len(parsed["shards"]), 2)

    def test_root_schema_is_closed(self):
        job = make_job("closed", shards=1)
        job["priority"] = "secret override"
        with self.assertRaisesRegex(ValidationError, "unknown"):
            parse_job(job)

    def test_duplicate_shard_is_rejected(self):
        job = make_job("duplicate", shards=2)
        job["shards"][1]["shard_id"] = job["shards"][0]["shard_id"]
        with self.assertRaisesRegex(ValidationError, "duplicate shard"):
            parse_job(job)

    def test_boolean_integer_is_rejected(self):
        job = make_job("bool", shards=1)
        job["policy"]["lease_ms"] = True
        with self.assertRaises(ValidationError):
            parse_job(job)

    def test_speculation_must_precede_lease(self):
        job = make_job("timing", shards=1)
        job["policy"]["lease_ms"] = 50
        job["policy"]["speculate_after_ms"] = 50
        with self.assertRaisesRegex(ValidationError, "less than"):
            parse_job(job)

    def test_non_finite_payload_is_rejected(self):
        job = make_job("nan", shards=1)
        job["shards"][0]["payload"]["value"] = math.nan
        with self.assertRaisesRegex(ValidationError, "finite"):
            parse_job(job)

    def test_deep_payload_is_rejected(self):
        job = make_job("deep", shards=1)
        value = None
        for _ in range(12):
            value = [value]
        job["shards"][0]["payload"] = value
        with self.assertRaisesRegex(ValidationError, "deep"):
            parse_job(job)

    def test_large_payload_is_rejected(self):
        job = make_job("large", shards=1)
        job["shards"][0]["payload"] = {"text": "x" * 17_000}
        with self.assertRaisesRegex(ValidationError, "exceeds"):
            parse_job(job)

    def test_too_many_shards_are_rejected(self):
        job = make_job("many", shards=1)
        job["shards"] = [
            {"shard_id": f"s-{index}", "payload": {}}
            for index in range(MAX_SHARDS + 1)
        ]
        with self.assertRaises(ValidationError):
            parse_job(job)

    def test_duplicate_json_keys_are_rejected(self):
        with self.assertRaisesRegex(ValidationError, "duplicate"):
            strict_json('{"job_id":"a","job_id":"b"}')

    def test_invalid_utf8_is_rejected(self):
        with self.assertRaisesRegex(ValidationError, "UTF-8"):
            strict_json(b"\xff")

    def test_oversized_json_is_rejected(self):
        with self.assertRaisesRegex(ValidationError, "exceeds"):
            strict_json(b"x" * 1_048_577)

    def test_output_shard_must_match(self):
        raw = json.dumps({"shard_id": "other", "value": 1}).encode()
        with self.assertRaisesRegex(ValidationError, "does not match"):
            parse_output(raw, "expected")

    def test_output_schema_is_closed(self):
        raw = json.dumps({"shard_id": "one", "value": 1, "extra": 2}).encode()
        with self.assertRaisesRegex(ValidationError, "unknown"):
            parse_output(raw, "one")

    def test_counters_are_validated(self):
        self.assertEqual(parse_counters({"records": 7}), {"records": 7})
        with self.assertRaises(ValidationError):
            parse_counters({"records": -1})

    def test_counter_booleans_are_rejected(self):
        with self.assertRaises(ValidationError):
            parse_counters({"records": True})


if __name__ == "__main__":
    unittest.main()
