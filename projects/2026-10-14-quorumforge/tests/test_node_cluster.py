import unittest

from quorumforge import ClusterConfig, ConflictError, QuorumCluster, UnavailableError, ValidationError, VectorClock
from quorumforge.model import Version
from quorumforge.node import merge_versions


class VersionMergeTests(unittest.TestCase):
    def version(self, value, clock):
        return Version(value, VectorClock.from_dict(clock), next(iter(clock)))

    def test_descendant_replaces_ancestor(self):
        old = self.version("old", {"n1": 1})
        new = self.version("new", {"n1": 2})
        self.assertEqual(merge_versions((old,), (new,)), (new,))
        self.assertEqual(merge_versions((new,), (old,)), (new,))

    def test_concurrent_versions_remain_siblings(self):
        left = self.version("left", {"n1": 1})
        right = self.version("right", {"n2": 1})
        self.assertEqual(len(merge_versions((left,), (right,))), 2)

    def test_equal_clock_with_different_data_is_corruption(self):
        left = self.version("left", {"n1": 1})
        right = self.version("right", {"n1": 1})
        with self.assertRaises(ConflictError):
            merge_versions((left,), (right,))


class ClusterTests(unittest.TestCase):
    def setUp(self):
        self.cluster = QuorumCluster(
            ("n1", "n2", "n3", "n4", "n5"),
            ClusterConfig(replication_factor=3, read_quorum=2, write_quorum=2, virtual_nodes=16, merkle_buckets=16),
        )

    def test_write_read_and_replication(self):
        result = self.cluster.put("cart:1", "one", coordinator="n1")
        self.assertGreaterEqual(result.acknowledgements, 2)
        read = self.cluster.get("cart:1", coordinator="n2")
        self.assertEqual([item.value for item in read.versions], ["one"])
        self.assertEqual(self.cluster.audit()["under_replicated_keys"], 0)

    def test_sloppy_quorum_creates_and_hands_off_hints(self):
        key = next(
            f"cart:{index}" for index in range(1000)
            if "n1" in self.cluster.intended_replicas(f"cart:{index}")
        )
        self.cluster.fail_node("n1")
        result = self.cluster.put(key, "value", coordinator="n2")
        self.assertTrue(result.hinted_replicas)
        self.assertGreater(self.cluster.audit()["pending_hints"], 0)
        self.cluster.recover_node("n1")
        handoff = self.cluster.handoff()
        self.assertGreater(handoff["keys_delivered"], 0)
        self.assertEqual(self.cluster.audit()["pending_hints"], 0)
        self.assertTrue(self.cluster.nodes["n1"].normal_read(key))

    def test_write_fails_when_quorum_cannot_be_reached(self):
        for node in ("n2", "n3", "n4", "n5"):
            self.cluster.fail_node(node)
        with self.assertRaisesRegex(UnavailableError, "write quorum"):
            self.cluster.put("cart:1", "value", coordinator="n1")
        self.assertEqual(self.cluster.metrics["writes_failed"], 1)

    def test_down_coordinator_is_rejected(self):
        self.cluster.fail_node("n1")
        with self.assertRaisesRegex(UnavailableError, "coordinator"):
            self.cluster.get("cart:1", coordinator="n1")

    def test_unknown_context_node_is_rejected(self):
        with self.assertRaisesRegex(ValidationError, "outside this cluster"):
            self.cluster.put("cart:1", "value", coordinator="n1", context={"evil": 9})

    def test_read_repair_restores_missing_reachable_replica(self):
        key = "cart:repair"
        self.cluster.put(key, "value", coordinator="n1")
        target = self.cluster.intended_replicas(key)[0]
        self.cluster.unsafe_drop(target, key)
        read = self.cluster.get(key, coordinator="n2")
        self.assertGreaterEqual(read.repairs, 1)
        self.assertTrue(self.cluster.nodes[target].normal_read(key))

    def test_delete_is_a_replicated_tombstone(self):
        created = self.cluster.put("cart:delete", "value", coordinator="n1")
        deleted = self.cluster.delete("cart:delete", coordinator="n2", context=created.clock)
        read = self.cluster.get("cart:delete", coordinator="n3")
        self.assertTrue(deleted.tombstone)
        self.assertFalse(read.found)
        self.assertEqual(len(read.versions), 1)
        self.assertTrue(read.versions[0].tombstone)

    def test_network_partition_creates_causal_siblings(self):
        key = "cart:branches"
        initial = self.cluster.put(key, "base", coordinator="n1")
        self.cluster.partition([("n1", "n3", "n5"), ("n2", "n4")])
        self.cluster.put(key, "left", coordinator="n1", context=initial.clock)
        self.cluster.put(key, "right", coordinator="n2", context=initial.clock)
        self.cluster.heal_network()
        result = self.cluster.get(key, coordinator="n1")
        self.assertEqual({item.value for item in result.versions}, {"left", "right"})
        self.cluster.put(key, "merged", coordinator="n1", context=result.context)
        self.assertEqual([item.value for item in self.cluster.get(key, coordinator="n1").versions], ["merged"])

    def test_merkle_repair_fixes_dropped_shared_key(self):
        for index in range(100):
            self.cluster.put(f"cart:{index}", str(index), coordinator="n1")
        pair = None
        for left in self.cluster.nodes:
            for right in self.cluster.nodes:
                if left < right and self.cluster.shared_replica_keys(left, right):
                    pair = (left, right)
                    break
            if pair:
                break
        key = sorted(self.cluster.shared_replica_keys(*pair))[0]
        self.cluster.unsafe_drop(pair[1], key)
        result = self.cluster.anti_entropy_pair(*pair)
        self.assertEqual(result["keys_repaired"], 1)
        self.assertTrue(result["root_equal_after"])

    def test_anti_entropy_requires_reachable_distinct_nodes(self):
        with self.assertRaises(ValidationError):
            self.cluster.anti_entropy_pair("n1", "n1")
        self.cluster.fail_node("n2")
        with self.assertRaises(UnavailableError):
            self.cluster.anti_entropy_pair("n1", "n2")

    def test_configuration_and_partition_validation(self):
        with self.assertRaises(ValidationError):
            QuorumCluster(("n1", "n2", "n3"), ClusterConfig(replication_factor=4))
        with self.assertRaises(ValidationError):
            self.cluster.partition([("n1",), ("n2",)])

    def test_value_and_key_limits(self):
        for key, value in [("bad key", "x"), ("ok", "x" * 8193), ("", "x")]:
            with self.subTest(key=key), self.assertRaises(ValidationError):
                self.cluster.put(key, value, coordinator="n1")


if __name__ == "__main__":
    unittest.main()
