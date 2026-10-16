from concurrent.futures import ThreadPoolExecutor
import random
import sqlite3
import subprocess
import sys
import threading

from shiftsafe import Engine
from shiftsafe.errors import Busy, Conflict
from .helpers import EngineCase, item


class RecoveryAndConcurrencyTests(EngineCase):
    def test_abrupt_process_exit_between_dual_writes_has_no_partial_commit(self):
        self.seed(1)
        self.engine.start()
        script = """
import os,sys
from shiftsafe import Engine
def fault(point):
    if point == 'after_legacy_write': os._exit(17)
Engine(sys.argv[1], fault=fault).put(1, [], operation_id='crash')
"""
        process = subprocess.run([sys.executable, "-c", script, self.path], timeout=10, capture_output=True)
        self.assertEqual(process.returncode, 17, process.stderr.decode())
        restarted = Engine(self.path)
        self.assertEqual(restarted.get(1)["subscriptions"], item())
        self.assertEqual(restarted.status()["operations"], 1)
        self.assertEqual(restarted.status()["projection_state"], 0)
        self.assertFalse(restarted.put(1, [], operation_id="crash")["replayed"])
        self.assertTrue(restarted.verify()["ok"])

    def test_abrupt_process_exit_after_checkpoint_recovers_projection_and_cursor(self):
        self.seed(5)
        self.engine.start()
        script = """
import os,sys
from shiftsafe import Engine
def fault(point):
    if point == 'after_batch_checkpoint': os._exit(18)
e=Engine(sys.argv[1],clock=lambda:100.0,fault=fault)
l=e.acquire('crasher',ttl=1)
e.backfill(l,batch_size=3)
"""
        process = subprocess.run([sys.executable, "-c", script, self.path], timeout=10, capture_output=True)
        self.assertEqual(process.returncode, 18, process.stderr.decode())
        self.assertEqual(self.engine.status()["cursor"], 0)
        self.assertEqual(self.engine.status()["projection_state"], 0)
        self.clock[0] = 102
        lease = self.engine.acquire("recovery")
        self.engine.backfill(lease)
        self.assertTrue(self.engine.verify()["ok"])

    def test_four_writers_with_revision_checks_do_not_lose_updates_during_backfill(self):
        self.seed(50)
        self.engine.start()
        lease = self.engine.acquire("backfiller", ttl=300)
        start = threading.Event()

        def increment(worker_id):
            engine = Engine(self.path, clock=lambda: self.clock[0])
            start.wait(5)
            for step in range(30):
                for _ in range(1000):
                    current = engine.get(1)
                    value = current["subscriptions"][0]["amount_cents"]
                    try:
                        engine.put(1, item(amount=value+1), operation_id=f"w{worker_id}:{step}",
                                   expected_revision=current["revision"])
                        break
                    except Conflict:
                        continue
                else:
                    raise AssertionError("writer failed to make progress")

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(increment, i) for i in range(4)]
            start.set()
            while not self.engine.backfill(lease, batch_size=5)["done"]:
                pass
            for future in futures:
                future.result(timeout=30)
        self.assertTrue(self.engine.verify()["ok"])
        self.assertEqual(self.engine.get(1)["subscriptions"], item(amount=220))
        self.assertEqual(self.engine.get(1)["revision"], 121)
        self.assertEqual(self.engine.status()["operations"], 170)

    def test_cutover_gate_holds_writer_lock_until_phase_switch_commits(self):
        self.ready(1)
        entered, permit, writer_done = threading.Event(), threading.Event(), threading.Event()
        def hook(point):
            if point == "before_gate_commit":
                entered.set()
                if not permit.wait(5):
                    raise RuntimeError("test gate timed out")
        gate = Engine(self.path, fault=hook)
        writer = Engine(self.path)
        def write():
            writer.put(1, item(amount=900), operation_id="after-gate")
            writer_done.set()
        with ThreadPoolExecutor(max_workers=2) as pool:
            cutover = pool.submit(gate.cutover)
            self.assertTrue(entered.wait(5))
            mutation = pool.submit(write)
            try:
                self.assertFalse(writer_done.wait(0.05))
            finally:
                permit.set()
            report = cutover.result(timeout=10)
            mutation.result(timeout=10)
        self.assertEqual(report["data_version"], 1)
        self.assertEqual(self.engine.status()["data_version"], 2)
        self.assertEqual(self.engine.get(1)["subscriptions"], item(amount=900))
        self.assertTrue(self.engine.verify()["ok"])

    def test_lock_timeout_is_an_explicit_retryable_error(self):
        self.seed(1)
        engine = Engine(self.path, timeout=0.01)
        con = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(con.close)
        con.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(Busy):
                engine.put(1, item(amount=321), operation_id="retry")
        finally:
            con.rollback()
        self.assertFalse(engine.put(1, item(amount=321), operation_id="retry")["replayed"])

    def test_seeded_model_matches_through_backfill_rollback_and_finalization(self):
        rng, model = random.Random(817), {}
        for cid in range(1, 21):
            self.engine.put(cid, item(cid), operation_id=f"initial:{cid}")
            model[cid] = (1, False, item(cid))
        self.engine.start()
        lease = self.engine.acquire("model")
        for step in range(300):
            cid = rng.randint(1, 30)
            old_rev = model.get(cid, (0, False, []))[0]
            deleting = cid in model and rng.random() < 0.2
            payload = [] if deleting else item(cid, rng.randint(0, 100000))
            if deleting:
                self.engine.delete(cid, operation_id=f"model:{step}", expected_revision=old_rev)
            else:
                self.engine.put(cid, payload, operation_id=f"model:{step}", expected_revision=old_rev)
            model[cid] = (old_rev + 1, deleting, payload)
            if step < 20 and step % 2 == 0:
                self.engine.backfill(lease, batch_size=2)
            if step == 60:
                while not self.engine.backfill(lease, batch_size=3)["done"]:
                    pass
                self.engine.release(lease)
                self.engine.shadow()
            if step == 100:
                self.engine.cutover()
            if step == 150:
                self.engine.rollback()
                self.engine.shadow()
            if step == 170:
                self.engine.cutover()
            if step == 210:
                self.engine.finalize()
                while self.engine.cleanup(batch_size=5):
                    pass
            observed = self.engine.get(cid)
            self.assertEqual((observed["revision"], observed["deleted"], observed["subscriptions"]), model[cid])
            if 61 <= step < 210:
                self.assertTrue(self.engine.verify()["ok"])
        for cid, expected in model.items():
            actual = self.engine.get(cid)
            self.assertEqual((actual["revision"], actual["deleted"], actual["subscriptions"]), expected)
        self.assertEqual(self.engine.status()["legacy_customers"], 0)
