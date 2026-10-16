from shiftsafe import Engine
from shiftsafe.errors import LeaseBusy, LeaseLost, StaleBatch, ValidationError
from .helpers import EngineCase, item


class BackfillTests(EngineCase):
    def setUp(self):
        super().setUp()
        self.seed(8)
        self.engine.start()

    def test_stale_snapshot_never_overwrites_a_live_update(self):
        lease = self.engine.acquire("worker")
        batch = self.engine.capture(lease, batch_size=4)
        self.engine.put(1, item(amount=500), operation_id="live")
        result = self.engine.apply(lease, batch)
        self.assertEqual(result["skipped_newer_or_equal"], 1)
        self.assertEqual(self.engine.get(1)["subscriptions"], item(amount=500))
        while not self.engine.backfill(lease, batch_size=4)["done"]:
            pass
        self.assertTrue(self.engine.verify()["ok"])

    def test_stale_snapshot_does_not_resurrect_deleted_subscriptions(self):
        lease = self.engine.acquire("worker")
        batch = self.engine.capture(lease, batch_size=4)
        self.engine.delete(2, operation_id="delete")
        self.engine.apply(lease, batch)
        while not self.engine.backfill(lease)["done"]:
            pass
        self.assertTrue(self.engine.verify()["ok"])
        self.engine.shadow()
        self.engine.cutover()
        self.assertTrue(self.engine.get(2)["deleted"])
        self.assertEqual(self.engine.get(2)["subscriptions"], [])

    def test_new_customer_below_cursor_is_still_dual_written(self):
        # Existing IDs have gaps; a new ID can appear behind a durable cursor.
        path = f"{self.folder.name}/gaps.db"
        engine = Engine(path)
        engine.put(10, item(10), operation_id="ten")
        engine.start()
        lease = engine.acquire("worker")
        engine.backfill(lease)
        engine.put(1, item(1), operation_id="behind")
        engine.put(999, item(999), operation_id="ahead")
        self.assertTrue(engine.verify()["ok"])
        engine.shadow()
        engine.cutover()
        self.assertEqual(engine.get(1)["subscriptions"], item(1))
        self.assertEqual(engine.get(999)["subscriptions"], item(999))

    def test_exact_batch_boundary_finishes_at_watermark(self):
        lease = self.engine.acquire("worker")
        self.assertFalse(self.engine.backfill(lease, batch_size=4)["done"])
        self.assertTrue(self.engine.backfill(lease, batch_size=4)["done"])

    def test_checkpoint_and_projection_rollback_together_at_both_fault_points(self):
        lease = self.engine.acquire("worker")
        batch = self.engine.capture(lease, batch_size=4)
        for target in ("after_batch_projection", "after_batch_checkpoint"):
            def fail(point, target=target):
                if point == target:
                    raise RuntimeError("batch crash")
            faulty = Engine(self.path, clock=lambda: self.clock[0], fault=fail)
            with self.subTest(target=target), self.assertRaises(RuntimeError):
                faulty.apply(lease, batch)
            self.assertEqual(self.engine.status()["cursor"], 0)
            self.assertEqual(self.engine.status()["projection_state"], 0)
        self.engine.apply(lease, batch)
        self.assertEqual(self.engine.status()["cursor"], 4)

    def test_restart_resumes_from_last_committed_batch(self):
        lease = self.engine.acquire("first", ttl=5)
        self.engine.backfill(lease, batch_size=3)
        self.clock[0] += 6
        restarted = Engine(self.path, clock=lambda: self.clock[0])
        next_lease = restarted.acquire("second")
        next_batch = restarted.capture(next_lease, batch_size=3)
        self.assertEqual(next_batch.cursor_before, 3)
        self.assertEqual([r.customer_id for r in next_batch.rows], [4, 5, 6])

    def test_expired_worker_is_fenced_even_if_it_has_captured_data(self):
        first = self.engine.acquire("same-name", ttl=5)
        batch = self.engine.capture(first)
        self.clock[0] += 5
        second = self.engine.acquire("same-name")
        self.assertGreater(second.fence, first.fence)
        with self.assertRaises(LeaseLost):
            self.engine.apply(first, batch)
        with self.assertRaises(LeaseLost):
            self.engine.release(first)
        with self.assertRaises(LeaseLost):
            self.engine.renew(first)
        self.assertTrue(self.engine.backfill(second)["done"])

    def test_unexpired_worker_blocks_a_competing_claim(self):
        self.engine.acquire("a")
        with self.assertRaises(LeaseBusy):
            self.engine.acquire("b")

    def test_renewal_protects_live_worker_until_new_expiry(self):
        lease = self.engine.acquire("a", ttl=5)
        self.clock[0] += 4
        self.engine.renew(lease, ttl=10)
        self.clock[0] += 4
        self.assertTrue(self.engine.backfill(lease)["done"])

    def test_batch_from_old_lease_cannot_be_reused_by_new_holder(self):
        first = self.engine.acquire("a")
        batch = self.engine.capture(first)
        self.engine.release(first)
        second = self.engine.acquire("b")
        with self.assertRaises(StaleBatch):
            self.engine.apply(second, batch)

    def test_replaying_committed_batch_cannot_rewind_checkpoint(self):
        lease = self.engine.acquire("a")
        batch = self.engine.capture(lease, batch_size=3)
        self.engine.apply(lease, batch)
        with self.assertRaises(StaleBatch):
            self.engine.apply(lease, batch)
        self.assertEqual(self.engine.status()["cursor"], 3)

    def test_worker_lease_cannot_write_after_shadow_transition(self):
        lease = self.engine.acquire("a")
        self.engine.backfill(lease)
        batch = self.engine.capture(lease)
        self.engine.shadow()
        with self.assertRaises(LeaseLost):
            self.engine.apply(lease, batch)

    def test_invalid_batch_sizes_and_ttls_are_rejected(self):
        lease = self.engine.acquire("a")
        for size in (0, -1, 1001, True, 1.5):
            with self.subTest(size=size), self.assertRaises(ValidationError):
                self.engine.capture(lease, batch_size=size)
        for ttl in (0, -1, 301, float("nan"), float("inf"), True):
            with self.subTest(ttl=ttl), self.assertRaises(ValidationError):
                self.engine.renew(lease, ttl=ttl)
