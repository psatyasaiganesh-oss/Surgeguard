from pathlib import Path
import sqlite3
import tempfile
import unittest

from shiftsafe import Engine


def item(cid=1, amount=100):
    return [{"id": f"sub_{cid}", "plan": "basic", "amount_cents": amount,
             "currency": "INR", "status": "active"}]


class EngineCase(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = str(Path(self.folder.name) / "lab.db")
        self.clock = [100.0]
        self.engine = Engine(self.path, clock=lambda: self.clock[0])

    def seed(self, count=5):
        for cid in range(1, count + 1):
            self.engine.put(cid, item(cid), operation_id=f"seed:{cid}")

    def ready(self, count=5):
        self.seed(count)
        self.engine.start()
        lease = self.engine.acquire("ready")
        while not self.engine.backfill(lease, batch_size=2)["done"]:
            pass
        self.engine.release(lease)
        self.engine.shadow()

    def corrupt(self, cid=1):
        with sqlite3.connect(self.path) as con:
            con.execute("UPDATE subscriptions_v2 SET amount_cents=amount_cents+10 WHERE customer_id=?", (cid,))
