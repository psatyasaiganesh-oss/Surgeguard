import copy
import json
import unittest

from routeproof.graph import AnalysisFailure, analyze
from routeproof.model import ValidationError, parse_plan, parse_topology, strict_json
from routeproof.samples import safe_plan, sample_topology


class ModelTests(unittest.TestCase):
    def test_valid_topology(self):
        self.assertEqual(parse_topology(sample_topology())["version"], 1)

    def test_topology_rejects_unknown_field(self):
        value = sample_topology()
        value["surprise"] = True
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_topology_rejects_boolean_version(self):
        value = sample_topology()
        value["version"] = True
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_duplicate_node_is_rejected(self):
        value = sample_topology()
        value["nodes"].append(copy.deepcopy(value["nodes"][0]))
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_unknown_link_endpoint_is_rejected(self):
        value = sample_topology()
        value["links"][0]["a"] = "missing"
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_duplicate_link_id_is_rejected(self):
        value = sample_topology()
        value["links"][1]["id"] = value["links"][0]["id"]
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_duplicate_endpoint_domain_is_rejected(self):
        value = sample_topology()
        value["links"][1]["failure_domain"] = value["links"][0]["failure_domain"]
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_requirement_kind_is_checked(self):
        value = sample_topology()
        value["requirements"]["core"] = "dc-a"
        with self.assertRaises(ValidationError):
            parse_topology(value)

    def test_plan_rejects_unknown_field(self):
        value = safe_plan()
        value["force"] = True
        with self.assertRaises(ValidationError):
            parse_plan(value)

    def test_plan_rejects_short_reason(self):
        value = safe_plan()
        value["reason"] = "short"
        with self.assertRaises(ValidationError):
            parse_plan(value)

    def test_plan_rejects_boolean_expected_version(self):
        value = safe_plan()
        value["expected_topology_version"] = True
        with self.assertRaises(ValidationError):
            parse_plan(value)

    def test_plan_rejects_too_many_steps(self):
        value = safe_plan()
        value["steps"] = value["steps"] * 51
        with self.assertRaises(ValidationError):
            parse_plan(value)

    def test_strict_json_rejects_duplicate_keys(self):
        with self.assertRaisesRegex(ValidationError, "duplicate"):
            strict_json('{"plan_id":"one","plan_id":"two"}')

    def test_strict_json_rejects_oversize(self):
        with self.assertRaisesRegex(ValidationError, "exceeds"):
            strict_json(b"x" * 65_537)

    def test_strict_json_round_trip(self):
        value = {"a": [1, 2], "b": True}
        self.assertEqual(strict_json(json.dumps(value)), value)

    def test_unknown_plan_link_fails_analysis(self):
        value = safe_plan()
        value["steps"][0]["link_id"] = "does-not-exist"
        with self.assertRaises(AnalysisFailure):
            analyze(sample_topology(), value)


if __name__ == "__main__":
    unittest.main()
