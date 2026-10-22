"""Deterministic local scenarios and a synthetic convergence benchmark."""

from __future__ import annotations

import copy
import hashlib
import tempfile
import time
from pathlib import Path
from typing import Any

from .model import IntegrityError
from .store import RegionStore
from .sync import converge, deliver_batch, receive_envelope, sign_operation, whole_document_lww


def demo_secrets(regions: list[str]) -> dict[str, bytes]:
    return {
        region: hashlib.sha256(f"mergemesh-local-demo:{region}".encode("utf-8")).digest()
        for region in regions
    }


def _document_etags(stores: list[RegionStore], tenant: str, document: str) -> list[str]:
    return [store.get_document(tenant, document)["etag"] for store in stores]


def _benchmark(root: Path, *, document_count: int = 120) -> dict[str, Any]:
    regions = ["bench-east", "bench-west", "bench-central"]
    stores = [RegionStore(root / f"{region}.db", region) for region in regions]
    secrets = demo_secrets(regions)
    tenant = "benchmark"

    for index in range(document_count):
        stores[0].patch(
            tenant,
            f"sku-{index:04d}",
            changes={"price": index, "availability": "IN_STOCK", "category": "base"},
        )
    seed_sync = converge(stores, secrets)
    if not seed_sync["converged"]:
        raise RuntimeError("benchmark seed did not converge")

    started = time.perf_counter()
    for index in range(document_count):
        document = f"sku-{index:04d}"
        stores[0].patch(tenant, document, changes={"price": index + 1})
        stores[1].patch(
            tenant,
            document,
            changes={"availability": "BACKORDER" if index % 7 == 0 else "IN_STOCK"},
        )
        stores[2].patch(tenant, document, changes={"category": f"group-{index % 5}"})
    sync = converge(stores, secrets)
    elapsed = time.perf_counter() - started

    correct = True
    same_etags = True
    for index in range(document_count):
        document = f"sku-{index:04d}"
        snapshots = [store.get_document(tenant, document) for store in stores]
        expected = {
            "price": index + 1,
            "availability": "BACKORDER" if index % 7 == 0 else "IN_STOCK",
            "category": f"group-{index % 5}",
        }
        correct = correct and all(snapshot["values"] == expected for snapshot in snapshots)
        same_etags = same_etags and len({snapshot["etag"] for snapshot in snapshots}) == 1
    integrity = [store.verify() for store in stores]
    logical_mutations = document_count * 3
    return {
        "documents": document_count,
        "logical_mutations": logical_mutations,
        "seconds": round(elapsed, 6),
        "logical_mutations_per_second": round(logical_mutations / elapsed, 2),
        "replication_operations_accepted": sync["accepted"],
        "converged": sync["converged"] and correct and same_etags,
        "unresolved_conflicts": sum(store.stats()["unresolved_conflicts"] for store in stores),
        "integrity_verified": all(result["ok"] for result in integrity),
    }


def run_demo(workdir: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if workdir is None:
        temporary = tempfile.TemporaryDirectory(prefix="mergemesh-")
        root = Path(temporary.name)
    else:
        root = Path(workdir)
        root.mkdir(parents=True, exist_ok=True)

    try:
        regions = ["east", "west", "central"]
        east, west, central = [RegionStore(root / f"{region}.db", region) for region in regions]
        stores = [east, west, central]
        secrets = demo_secrets(regions)
        tenant = "acme"
        document = "keyboard-1"

        base = {"price": 100, "availability": "IN_STOCK", "title": "Keyboard"}
        east.patch(tenant, document, changes=base)
        converge(stores, secrets)

        # Two regions edit different fields while disconnected.
        east.patch(tenant, document, changes={"price": 90})
        west.patch(tenant, document, changes={"availability": "BACKORDER"})
        lww = whole_document_lww(
            [
                {
                    "timestamp": 10,
                    "region": "east",
                    "document": {**base, "price": 90},
                },
                {
                    "timestamp": 11,
                    "region": "west",
                    "document": {**base, "availability": "BACKORDER"},
                },
            ]
        )
        disjoint_sync = converge(stores, secrets)
        merged = central.get_document(tenant, document)
        disjoint_preserved = (
            merged["values"]["price"] == 90
            and merged["values"]["availability"] == "BACKORDER"
            and merged["conflict_count"] == 0
        )

        # A true same-field conflict is retained instead of silently choosing a winner.
        east.patch(tenant, document, changes={"price": 85})
        west.patch(tenant, document, changes={"price": 88})
        conflict_sync = converge(stores, secrets)
        conflicted = central.get_document(tenant, document)
        price_candidates = [
            candidate["op_id"]
            for candidate in conflicted["fields"]["price"]["candidates"]
        ]
        conflict_visible = (
            conflicted["fields"]["price"]["status"] == "conflict"
            and len(price_candidates) == 2
        )
        central.resolve(
            tenant,
            document,
            "price",
            expected_candidates=price_candidates,
            value=87,
        )
        resolution_sync = converge(stores, secrets)
        resolved = [store.get_document(tenant, document) for store in stores]
        resolution_converged = all(
            snapshot["values"]["price"] == 87 and not snapshot["conflicts"]
            for snapshot in resolved
        )

        # A concurrent delete/update remains explicit and is resolved with a precondition.
        east.patch(tenant, document, delete_fields=["note"])
        west.patch(tenant, document, changes={"note": "expedite"})
        converge(stores, secrets)
        delete_conflict = central.get_document(tenant, document)
        note_candidates = [
            candidate["op_id"]
            for candidate in delete_conflict["fields"]["note"]["candidates"]
        ]
        central.resolve(
            tenant,
            document,
            "note",
            expected_candidates=note_candidates,
            delete=True,
        )
        converge(stores, secrets)
        delete_won_explicitly = all(
            "note" in store.get_document(tenant, document)["deleted_fields"]
            for store in stores
        )

        # Receiver commits, acknowledgement is lost, and retry deduplicates the operation.
        new_operation = central.patch(
            tenant, document, changes={"shipping_class": "priority"}
        )["op_id"]
        first_delivery = deliver_batch(
            central, east, secrets, limit=1000, acknowledge=False
        )
        retry_delivery = deliver_batch(central, east, secrets, limit=1000)
        lost_ack_recovered = (
            new_operation in first_delivery["operation_ids"]
            and first_delivery["accepted"] == 1
            and retry_delivery["duplicates"] >= 1
            and east.get_document(tenant, document)["values"]["shipping_class"]
            == "priority"
        )
        converge(stores, secrets)

        # Authentication is checked before a remote operation reaches SQLite.
        envelope = sign_operation(central.operation(new_operation), secrets["central"])
        tampered = copy.deepcopy(envelope)
        tampered["operation"]["mutations"]["shipping_class"]["value"] = "tampered"
        tamper_rejected = False
        try:
            receive_envelope(west, tampered, secrets)
        except IntegrityError:
            tamper_rejected = True

        # Reopen from disk to prove that state and causal metadata survive restart.
        reopened = RegionStore(root / "east.db", "east")
        restart_state = reopened.get_document(tenant, document)
        restart_verified = (
            restart_state["values"]["price"] == 87
            and restart_state["values"]["shipping_class"] == "priority"
            and reopened.verify()["ok"]
        )

        etags = _document_etags(stores, tenant, document)
        verification = [store.verify() for store in stores]
        benchmark = _benchmark(root / "benchmark")
        result = {
            "project": "MergeMesh",
            "scenario": {
                "whole_document_lww": {
                    "winner_region": lww["region"],
                    "winner": lww["document"],
                    "lost_disjoint_price_edit": lww["document"]["price"] != 90,
                },
                "field_aware_merge": {
                    "values": {
                        "price": merged["values"]["price"],
                        "availability": merged["values"]["availability"],
                    },
                    "disjoint_edits_preserved": disjoint_preserved,
                    "sync_converged": disjoint_sync["converged"],
                },
                "same_field_conflict": {
                    "candidates": len(price_candidates),
                    "surfaced": conflict_visible,
                    "sync_converged": conflict_sync["converged"],
                    "resolution_value": 87,
                    "resolution_converged": resolution_converged
                    and resolution_sync["converged"],
                },
                "delete_update_conflict": {
                    "candidates": len(note_candidates),
                    "explicit_delete_resolution_converged": delete_won_explicitly,
                },
                "lost_ack_retry": {
                    "first_delivery_accepted": first_delivery["accepted"],
                    "retry_duplicates": retry_delivery["duplicates"],
                    "recovered_without_duplicate_version": lost_ack_recovered,
                },
                "tampered_replication_rejected": tamper_rejected,
                "restart_recovered": restart_verified,
                "replicas_share_etag": len(set(etags)) == 1,
            },
            "benchmark": benchmark,
            "verification": {
                "all_replicas_valid": all(item["ok"] for item in verification),
                "replica_stats": [store.stats() for store in stores],
            },
        }
        checks = [
            result["scenario"]["whole_document_lww"]["lost_disjoint_price_edit"],
            disjoint_preserved,
            conflict_visible,
            result["scenario"]["same_field_conflict"]["resolution_converged"],
            delete_won_explicitly,
            lost_ack_recovered,
            tamper_rejected,
            restart_verified,
            result["scenario"]["replicas_share_etag"],
            benchmark["converged"],
            benchmark["integrity_verified"],
            result["verification"]["all_replicas_valid"],
        ]
        if not all(checks):
            raise RuntimeError(f"demo invariant failed: {checks}")
        return result
    finally:
        if temporary is not None:
            temporary.cleanup()
