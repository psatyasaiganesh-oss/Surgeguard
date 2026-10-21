import os
import tempfile
import unittest
from pathlib import Path

from specuflow.blob import BlobStore, IntegrityError
from specuflow.model import ValidationError


class BlobTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = BlobStore(Path(self.temp.name) / "blobs")

    def tearDown(self):
        self.temp.cleanup()

    def test_round_trip(self):
        digest = self.store.put(b"result")
        self.assertEqual(self.store.get(digest), b"result")

    def test_identical_put_is_idempotent(self):
        self.assertEqual(self.store.put(b"same"), self.store.put(b"same"))

    def test_bad_digest_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.get("../escape")

    def test_missing_blob_is_detected(self):
        with self.assertRaises(IntegrityError):
            self.store.get("0" * 64)

    def test_corruption_is_detected(self):
        digest = self.store.put(b"original")
        self.store._path(digest).write_bytes(b"tampered")
        with self.assertRaisesRegex(IntegrityError, "checksum"):
            self.store.get(digest)

    def test_empty_blob_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.put(b"")

    def test_non_bytes_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.put("text")  # type: ignore[arg-type]

    @unittest.skipIf(os.name == "nt", "POSIX mode check")
    def test_blob_directory_is_private(self):
        mode = os.stat(Path(self.temp.name) / "blobs").st_mode & 0o777
        self.assertEqual(mode, 0o700)


if __name__ == "__main__":
    unittest.main()
