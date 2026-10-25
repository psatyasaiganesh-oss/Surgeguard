"""Deterministic, zone-aware shuffle-shard assignment and impact analysis."""

from __future__ import annotations

import hashlib
import math
from collections import Counter
from typing import Iterable

from .errors import ValidationError
from .model import Assignment, Cell, canonical_json, digest_json, validate_cells, validate_id


class ShuffleTopology:
    def __init__(
        self,
        cells: Iterable[Cell],
        *,
        shard_size: int = 2,
        salt: str = "cellguard-v1",
        zone_spread: bool = True,
    ) -> None:
        self.cells = validate_cells(cells)
        if isinstance(shard_size, bool) or not isinstance(shard_size, int):
            raise ValidationError("shard_size must be an integer")
        if shard_size < 2 or shard_size > len(self.cells):
            raise ValidationError("shard_size must be between 2 and the cell count")
        self.shard_size = shard_size
        self.salt = validate_id(salt, "salt")
        if not isinstance(zone_spread, bool):
            raise ValidationError("zone_spread must be boolean")
        self.zone_spread = zone_spread
        self.epoch = digest_json(
            {
                "cells": [cell.as_dict() for cell in self.cells],
                "salt": self.salt,
                "shard_size": self.shard_size,
                "zone_spread": self.zone_spread,
            }
        )

    def _score(self, tenant: str, cell: Cell) -> int:
        material = f"{self.salt}\0{tenant}\0{cell.cell_id}".encode("utf-8")
        return int.from_bytes(hashlib.sha256(material).digest(), "big")

    def assign(self, tenant: str) -> Assignment:
        tenant = validate_id(tenant, "tenant")
        ranked = sorted(
            self.cells,
            key=lambda cell: (-self._score(tenant, cell), cell.cell_id),
        )
        selected: list[Cell] = []
        if self.zone_spread:
            used_zones: set[str] = set()
            for cell in ranked:
                if cell.zone not in used_zones:
                    selected.append(cell)
                    used_zones.add(cell.zone)
                    if len(selected) == min(self.shard_size, len({c.zone for c in self.cells})):
                        break
        for cell in ranked:
            if cell not in selected:
                selected.append(cell)
            if len(selected) == self.shard_size:
                break
        cell_ids = tuple(sorted(cell.cell_id for cell in selected))
        material = {"cells": list(cell_ids), "epoch": self.epoch, "tenant": tenant}
        return Assignment(tenant, self.epoch, cell_ids, digest_json(material))

    def assignments(self, tenants: Iterable[str]) -> dict[str, Assignment]:
        result: dict[str, Assignment] = {}
        for tenant in tenants:
            tenant = validate_id(tenant, "tenant")
            if tenant in result:
                raise ValidationError("tenant identifiers must be unique")
            result[tenant] = self.assign(tenant)
        return result

    def impact_report(self, tenants: Iterable[str], noisy_tenant: str) -> dict[str, object]:
        assignment_map = self.assignments(tenants)
        noisy_tenant = validate_id(noisy_tenant, "noisy_tenant")
        if noisy_tenant not in assignment_map:
            raise ValidationError("noisy_tenant must be included in tenants")
        failed = set(assignment_map[noisy_tenant].cells)
        fully_impacted: list[str] = []
        partially_overlapping: list[str] = []
        unaffected: list[str] = []
        for tenant, assignment in assignment_map.items():
            overlap = len(failed.intersection(assignment.cells))
            if overlap == len(assignment.cells):
                fully_impacted.append(tenant)
            elif overlap:
                partially_overlapping.append(tenant)
            else:
                unaffected.append(tenant)
        combinations = Counter(assignment.cells for assignment in assignment_map.values())
        return {
            "cell_count": len(self.cells),
            "distinct_virtual_shards": len(combinations),
            "failed_cells": sorted(failed),
            "fully_impacted": len(fully_impacted),
            "fully_impacted_fraction": len(fully_impacted) / len(assignment_map),
            "noisy_tenant": noisy_tenant,
            "partially_overlapping": len(partially_overlapping),
            "shard_size": self.shard_size,
            "tenant_count": len(assignment_map),
            "unaffected": len(unaffected),
        }

    def theoretical_virtual_shards(self) -> int:
        if not self.zone_spread:
            return math.comb(len(self.cells), self.shard_size)
        # Dynamic programming counts combinations by selected cells and zones.
        # This avoids enumerating an exponentially large combination space.
        zones_needed = min(self.shard_size, len({cell.zone for cell in self.cells}))
        zone_sizes = Counter(cell.zone for cell in self.cells).values()
        counts = [[0] * (zones_needed + 1) for _ in range(self.shard_size + 1)]
        counts[0][0] = 1
        for zone_size in zone_sizes:
            updated = [[0] * (zones_needed + 1) for _ in range(self.shard_size + 1)]
            for selected in range(self.shard_size + 1):
                for used_zones in range(zones_needed + 1):
                    current = counts[selected][used_zones]
                    if not current:
                        continue
                    for take in range(min(zone_size, self.shard_size - selected) + 1):
                        next_zones = used_zones + (1 if take else 0)
                        if next_zones <= zones_needed:
                            updated[selected + take][next_zones] += current * math.comb(
                                zone_size, take
                            )
            counts = updated
        return counts[self.shard_size][zones_needed]

    def as_dict(self) -> dict[str, object]:
        return {
            "cells": [cell.as_dict() for cell in self.cells],
            "epoch": self.epoch,
            "shard_size": self.shard_size,
            "theoretical_virtual_shards": self.theoretical_virtual_shards(),
            "zone_spread": self.zone_spread,
        }
