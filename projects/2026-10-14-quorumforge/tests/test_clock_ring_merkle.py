import unittest

from quorumforge.clock import ClockRelation, MAX_COUNTER, VectorClock
from quorumforge.errors import ValidationError
from quorumforge.merkle import MerkleIndex
from quorumforge.ring import ConsistentHashRing


class VectorClockTests(unittest.TestCase):
    def test_increment_merge_and_serialization(self):
        left = VectorClock().increment("n1").increment("n1")
        right = VectorClock().increment("n2")
        merged = VectorClock.merge(left, right)
        self.assertEqual(merged.to_dict(), {"n1": 2, "n2": 1})
        self.assertEqual(VectorClock.from_dict(merged.to_dict()), merged)

    def test_all_causal_relations(self):
        base = VectorClock.from_dict({"n1": 1})
        child = VectorClock.from_dict({"n1": 2})
        concurrent = VectorClock.from_dict({"n2": 1})
        self.assertIs(base.relation(child), ClockRelation.BEFORE)
        self.assertIs(child.relation(base), ClockRelation.AFTER)
        self.assertIs(base.relation(VectorClock.from_dict({"n1": 1})), ClockRelation.EQUAL)
        self.assertIs(base.relation(concurrent), ClockRelation.CONCURRENT)

    def test_missing_entries_are_zero(self):
        left = VectorClock.from_dict({"n1": 1})
        right = VectorClock.from_dict({"n1": 1, "n2": 1})
        self.assertIs(left.relation(right), ClockRelation.BEFORE)

    def test_invalid_entries_and_exhaustion_are_rejected(self):
        invalid = [
            {"UPPER": 1},
            {"n1": 0},
            {"n1": True},
            {"n1": MAX_COUNTER + 1},
        ]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                VectorClock.from_dict(value)
        with self.assertRaises(ValidationError):
            VectorClock.from_dict({"n1": MAX_COUNTER}).increment("n1")

    def test_context_size_is_bounded(self):
        with self.assertRaises(ValidationError):
            VectorClock.from_dict({f"n{index}": 1 for index in range(65)})


class RingTests(unittest.TestCase):
    def test_preference_list_is_stable_and_unique(self):
        ring = ConsistentHashRing(("n1", "n2", "n3", "n4"), virtual_nodes=16)
        first = ring.preference_list("cart:123", 3)
        self.assertEqual(first, ring.preference_list("cart:123", 3))
        self.assertEqual(len(first), len(set(first)))

    def test_all_nodes_are_returned_when_count_is_omitted(self):
        ring = ConsistentHashRing(("n1", "n2", "n3"))
        self.assertEqual(set(ring.preference_list("key")), {"n1", "n2", "n3"})

    def test_virtual_nodes_improve_this_deterministic_distribution(self):
        nodes = tuple(f"n{index}" for index in range(1, 9))
        keys = [f"key:{index}" for index in range(5000)]
        single = ConsistentHashRing(nodes, virtual_nodes=1).primary_counts(keys)
        virtual = ConsistentHashRing(nodes, virtual_nodes=64).primary_counts(keys)
        self.assertLess(max(virtual.values()) - min(virtual.values()), max(single.values()) - min(single.values()))

    def test_invalid_ring_configuration_is_rejected(self):
        for nodes, virtual_nodes in [(("n1", "n2"), 32), (("n1", "n1", "n2"), 32), (("n1", "n2", "n3"), 0)]:
            with self.subTest(nodes=nodes, virtual_nodes=virtual_nodes), self.assertRaises(ValidationError):
                ConsistentHashRing(nodes, virtual_nodes=virtual_nodes)


class MerkleTests(unittest.TestCase):
    def test_equal_indexes_compare_only_the_root(self):
        values = {"a": "0" * 64, "b": "1" * 64}
        difference = MerkleIndex(values, buckets=8).diff(MerkleIndex(values, buckets=8))
        self.assertEqual(difference.keys, ())
        self.assertEqual(difference.hash_nodes_compared, 1)

    def test_difference_returns_keys_only_from_changed_buckets(self):
        left = {f"key:{index}": f"{index:064x}" for index in range(20)}
        right = dict(left)
        right["key:7"] = "f" * 64
        difference = MerkleIndex(left, buckets=64).diff(MerkleIndex(right, buckets=64))
        self.assertIn("key:7", difference.keys)
        self.assertLess(len(difference.keys), len(left))
        self.assertGreater(difference.hash_nodes_compared, 1)

    def test_missing_key_is_detected(self):
        left = {"a": "0" * 64}
        difference = MerkleIndex(left, buckets=8).diff(MerkleIndex({}, buckets=8))
        self.assertEqual(difference.keys, ("a",))

    def test_bucket_count_must_match_and_be_power_of_two(self):
        with self.assertRaises(ValidationError):
            MerkleIndex({}, buckets=3)
        with self.assertRaises(ValidationError):
            MerkleIndex({}, buckets=8).diff(MerkleIndex({}, buckets=16))


if __name__ == "__main__":
    unittest.main()
