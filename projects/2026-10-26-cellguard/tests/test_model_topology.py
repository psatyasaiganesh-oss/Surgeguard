from __future__ import annotations

import math
import unittest

from cellguard.errors import ValidationError
from cellguard.model import Cell, canonical_json, validate_id, validate_integer
from cellguard.topology import ShuffleTopology


def topology(*, spread: bool = True) -> ShuffleTopology:
    return ShuffleTopology(
        [Cell(f"cell-{index}", f"zone-{index % 4}", 20) for index in range(8)],
        shard_size=2,
        zone_spread=spread,
    )


class ModelTests(unittest.TestCase):
    def test_validate_id_accepts_supported_characters(self) -> None:
        self.assertEqual(validate_id("tenant.a_b:c-1", "tenant"), "tenant.a_b:c-1")

    def test_validate_id_rejects_invalid_values(self) -> None:
        for value in ("", " space", "slash/value", "x" * 97, 4, None):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_id(value, "id")  # type: ignore[arg-type]

    def test_integer_rejects_boolean_and_range_errors(self) -> None:
        for value in (True, 0, 11, 1.5):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_integer(value, "count", 1, 10)  # type: ignore[arg-type]

    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"b": 2, "a": 1}), '{"a":1,"b":2}')

    def test_canonical_json_rejects_nonfinite_and_oversized_values(self) -> None:
        for value in (float("nan"), {"x": "a" * 17_000}, object()):
            with self.subTest(value=type(value).__name__), self.assertRaises(ValidationError):
                canonical_json(value)

    def test_cell_validates_capacity(self) -> None:
        with self.assertRaises(ValidationError):
            Cell("cell", "zone", 0).validated()


class TopologyTests(unittest.TestCase):
    def test_assignment_is_deterministic(self) -> None:
        first = topology().assign("tenant-a")
        self.assertEqual(first, topology().assign("tenant-a"))
        self.assertEqual(len(first.cells), 2)

    def test_zone_spread_uses_distinct_zones(self) -> None:
        current = topology()
        zones = {cell.cell_id: cell.zone for cell in current.cells}
        for index in range(100):
            assigned = current.assign(f"tenant-{index}").cells
            self.assertNotEqual(zones[assigned[0]], zones[assigned[1]])

    def test_theoretical_virtual_shards_respects_zone_spread(self) -> None:
        self.assertEqual(topology().theoretical_virtual_shards(), 24)
        self.assertEqual(topology(spread=False).theoretical_virtual_shards(), math.comb(8, 2))

    def test_assignments_reject_duplicate_tenants(self) -> None:
        with self.assertRaises(ValidationError):
            topology().assignments(["same", "same"])

    def test_impact_partitions_all_tenants(self) -> None:
        tenants = [f"tenant-{index}" for index in range(1000)]
        report = topology().impact_report(tenants, tenants[0])
        partitioned = (
            report["fully_impacted"] + report["partially_overlapping"] + report["unaffected"]
        )
        self.assertEqual(partitioned, len(tenants))
        self.assertLess(report["fully_impacted_fraction"], 0.1)

    def test_impact_requires_noisy_tenant_in_population(self) -> None:
        with self.assertRaises(ValidationError):
            topology().impact_report(["tenant-a"], "tenant-b")

    def test_invalid_topologies_are_rejected(self) -> None:
        cases = [
            ([Cell("only", "zone")], 2),
            ([Cell("same", "a"), Cell("same", "b")], 2),
            ([Cell("a", "a"), Cell("b", "b")], 1),
        ]
        for cells, size in cases:
            with self.subTest(cells=cells, size=size), self.assertRaises(ValidationError):
                ShuffleTopology(cells, shard_size=size)


if __name__ == "__main__":
    unittest.main()
