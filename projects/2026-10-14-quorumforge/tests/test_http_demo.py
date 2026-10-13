import http.client
import json
import unittest

from quorumforge import ClusterConfig, QuorumCluster
from quorumforge.demo import run_demo
from quorumforge.httpserver import ServerRuntime


class HTTPTests(unittest.TestCase):
    def setUp(self):
        cluster = QuorumCluster(
            ("n1", "n2", "n3", "n4", "n5"),
            ClusterConfig(replication_factor=3, read_quorum=2, write_quorum=2),
        )
        self.cluster = cluster
        self.runtime = ServerRuntime(cluster).start()
        self.host, self.port = self.runtime.address

    def tearDown(self):
        self.runtime.close()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection(self.host, self.port, timeout=3)
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.headers), json.loads(response.read())
        finally:
            connection.close()

    def json_request(self, method, path, payload):
        return self.request(
            method,
            path,
            json.dumps(payload, separators=(",", ":")),
            {"Content-Type": "application/json"},
        )

    def test_health_and_cluster_snapshot(self):
        status, headers, body = self.request("GET", "/health")
        self.assertEqual((status, body), (200, {"status": "ok"}))
        self.assertEqual(headers["Cache-Control"], "no-store")
        status, _, body = self.request("GET", "/cluster")
        self.assertEqual(status, 200)
        self.assertEqual(body["config"], {"N": 3, "R": 2, "W": 2, "merkle_buckets": 64, "virtual_nodes": 32})

    def test_put_get_update_and_delete(self):
        status, _, written = self.json_request("PUT", "/kv/cart:42?coordinator=n1", {"value": "first"})
        self.assertEqual(status, 201)
        status, _, read = self.request("GET", "/kv/cart:42?coordinator=n2")
        self.assertEqual((status, read["siblings"][0]["value"]), (200, "first"))
        status, _, updated = self.json_request(
            "PUT", "/kv/cart:42?coordinator=n2", {"value": "second", "context": read["context"]}
        )
        self.assertEqual(status, 201)
        status, _, deleted = self.json_request(
            "DELETE", "/kv/cart:42?coordinator=n3", {"context": updated["clock"]}
        )
        self.assertEqual((status, deleted["tombstone"]), (200, True))
        self.assertFalse(self.request("GET", "/kv/cart:42?coordinator=n1")[2]["found"])

    def test_invalid_requests_are_bounded_and_rejected(self):
        cases = [
            ("PUT", "/kv/cart:1?coordinator=n1", b"{}", {"Content-Type": "text/plain"}),
            ("PUT", "/kv/cart:1", b"{}", {"Content-Type": "application/json"}),
            ("PUT", "/kv/bad%20key?coordinator=n1", b'{"value":"x"}', {"Content-Type": "application/json"}),
            ("PUT", "/kv/cart:1?coordinator=n1", b'{"value":"x","value":"y"}', {"Content-Type": "application/json"}),
        ]
        for method, path, body, headers in cases:
            with self.subTest(path=path, body=body):
                status, _, result = self.request(method, path, body, headers)
                self.assertEqual(status, 400)
                self.assertEqual(result["error"], "invalid_request")

    def test_unavailable_quorum_returns_503(self):
        for node in ("n2", "n3", "n4", "n5"):
            self.cluster.fail_node(node)
        status, _, result = self.json_request("PUT", "/kv/cart:1?coordinator=n1", {"value": "x"})
        self.assertEqual((status, result["error"]), (503, "quorum_unavailable"))

    def test_server_rejects_non_loopback_binding(self):
        with self.assertRaisesRegex(Exception, "loopback"):
            ServerRuntime(self.cluster, host="0.0.0.0")


class DemoTests(unittest.TestCase):
    def test_demo_passes_every_invariant(self):
        report = run_demo(200, seed=14)
        self.assertTrue(all(report["invariants"].values()))
        self.assertEqual(report["availability_and_handoff"]["sloppy_quorum_successes"], 200)
        self.assertEqual(report["conflict_resolution"]["siblings_before_resolution"], 2)

    def test_demo_arguments_are_validated(self):
        for count in (99, True, 20_001):
            with self.subTest(count=count), self.assertRaises(ValueError):
                run_demo(count)
        with self.assertRaises(ValueError):
            run_demo(100, seed=True)


if __name__ == "__main__":
    unittest.main()
