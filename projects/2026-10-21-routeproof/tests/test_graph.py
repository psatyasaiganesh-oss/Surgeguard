import copy
import unittest

from routeproof.graph import analyze, invariant_violations
from routeproof.samples import safe_plan, sample_topology, unsafe_backbone_plan, unsafe_dns_plan


class GraphTests(unittest.TestCase):
    def test_sample_baseline_is_healthy(self):
        self.assertEqual(invariant_violations(sample_topology()), [])

    def test_backbone_disconnection_is_detected(self):
        topology = sample_topology()
        for link in topology["links"]:
            if link["id"].startswith("dc-a-backbone"):
                link["up"] = False
        codes = {item["code"] for item in invariant_violations(topology)}
        self.assertIn("BACKBONE_REACHABILITY", codes)

    def test_dns_withdrawal_capacity_is_detected(self):
        topology = sample_topology()
        for link in topology["links"]:
            if link["id"].startswith("dns-"):
                link["up"] = False
        codes = {item["code"] for item in invariant_violations(topology)}
        self.assertIn("DNS_ADVERTISEMENT_CAPACITY", codes)

    def test_management_path_loss_is_detected(self):
        topology = sample_topology()
        for link in topology["links"]:
            if link["id"].startswith("noc-dc-a"):
                link["up"] = False
        codes = {item["code"] for item in invariant_violations(topology)}
        self.assertIn("RECOVERY_PATH", codes)

    def test_safe_maintenance_passes(self):
        result = analyze(sample_topology(), safe_plan())
        self.assertTrue(result["safe"])
        self.assertEqual(result["violations"], [])

    def test_unsafe_backbone_plan_is_blocked(self):
        result = analyze(sample_topology(), unsafe_backbone_plan())
        self.assertFalse(result["safe"])
        codes = {item["code"] for item in result["violations"]}
        self.assertIn("SINGLE_FAILURE_TOLERANCE", codes)
        self.assertIn("STEP_INVARIANT", codes)

    def test_dns_cascade_is_blocked(self):
        result = analyze(sample_topology(), unsafe_dns_plan())
        self.assertFalse(result["safe"])
        direct = [
            detail
            for item in result["violations"]
            if item["code"] == "STEP_INVARIANT"
            for detail in item["details"]
        ]
        self.assertTrue(any(item["code"] == "DNS_ADVERTISEMENT_CAPACITY" for item in direct))

    def test_stale_topology_is_blocked_without_steps(self):
        result = analyze(sample_topology(3), safe_plan(2))
        self.assertFalse(result["safe"])
        self.assertEqual(result["steps"], [])
        self.assertEqual(result["violations"][0]["code"], "STALE_TOPOLOGY")

    def test_no_op_is_blocked(self):
        plan = safe_plan()
        plan["steps"][0]["up"] = True
        result = analyze(sample_topology(), plan)
        self.assertFalse(result["safe"])
        self.assertEqual(result["steps"][0]["violations"][0]["code"], "NO_OP")

    def test_repeated_operation_is_blocked(self):
        plan = safe_plan()
        plan["steps"].append(copy.deepcopy(plan["steps"][0]))
        result = analyze(sample_topology(), plan)
        self.assertFalse(result["safe"])
        self.assertIn("REPEATED_OPERATION", {item["code"] for item in result["violations"]})

    def test_shared_failure_domain_is_simulated_atomically(self):
        topology = sample_topology()
        second = next(item for item in topology["links"] if item["id"] == "dc-a-backbone-2")
        third = next(item for item in topology["links"] if item["id"] == "dc-a-backbone-3")
        second["failure_domain"] = "shared"
        third["failure_domain"] = "shared"
        third["b"] = "dc-b"
        result = analyze(topology, safe_plan())
        failures = result["steps"][0]["contingency_failures"]
        self.assertTrue(any(item["failure_domain"] == "shared" for item in failures))

    def test_single_failure_check_can_be_explicitly_disabled(self):
        topology = sample_topology()
        topology["requirements"]["require_single_failure_tolerance"] = False
        plan = safe_plan()
        plan["steps"].append({
            "action": "set_link",
            "link_id": "dc-a-backbone-2",
            "up": False,
        })
        result = analyze(topology, plan)
        self.assertTrue(result["safe"])

    def test_unhealthy_baseline_is_blocked(self):
        topology = sample_topology()
        for link in topology["links"]:
            if link["id"].startswith("noc-dc-c"):
                link["up"] = False
        result = analyze(topology, safe_plan())
        self.assertFalse(result["safe"])
        self.assertEqual(result["violations"][0]["code"], "UNHEALTHY_BASELINE")


if __name__ == "__main__":
    unittest.main()
