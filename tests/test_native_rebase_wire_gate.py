"""Packet/relay boundary tests; not MO COMMIT or actual killed-client proof."""

import socket
import tempfile
import threading
from pathlib import Path
import unittest

from tests.native_rebase_wire_gate import RebaseWireGate, read_packet


def packet(payload, sequence=0):
    return len(payload).to_bytes(3, "little") + bytes([sequence]) + payload


class Backend:
    def __init__(self, commit_error=False):
        self.requests = []
        self.commit_error = commit_error
        self.socket = socket.socket()
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(1)
        self.port = self.socket.getsockname()[1]
        self.client = None
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        try:
            self.client, _ = self.socket.accept()
            self.client.sendall(packet(b"\x0afixture\0", 0))
            read_packet(self.client)
            self.client.sendall(packet(b"\x00auth_ok", 2))
            while True:
                query = read_packet(self.client)[5:].decode()
                self.requests.append(query)
                reply = b"\xfferror" if self.commit_error and query == "COMMIT" else b"\x00ok"
                self.client.sendall(packet(reply, 1))
        except (OSError, EOFError):
            pass

    def close(self):
        for stream in (self.client, self.socket):
            if stream is not None:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                stream.close()
        self.thread.join(3)
        if self.thread.is_alive():
            raise AssertionError("test backend did not stop")


class WireGateTest(unittest.TestCase):
    def pair(self, mode, commit_error=False):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        backend = Backend(commit_error)
        self.addCleanup(backend.close)
        path = Path(tmp.name) / "wire.jsonl"
        gate = RebaseWireGate(backend.port, mode, path, snapshot_prefix="CREATE SNAPSHOT `fixed`",
                              success_prefix="UPDATE `meta`.`rebase_attempts` SET state=")
        self.addCleanup(gate.close)
        client = socket.create_connection(("127.0.0.1", gate.port), timeout=3)
        self.addCleanup(client.close)
        read_packet(client)
        client.sendall(packet(b"private_auth_bytes", 1))
        self.assertEqual(read_packet(client)[4:], b"\x00auth_ok")
        return backend, gate, client, path

    def arm(self, client):
        query = "UPDATE `meta`.`rebase_attempts` SET state='COMMITTED'"
        client.sendall(packet(b"\x03" + query.encode()))
        self.assertEqual(read_packet(client)[4:], b"\x00ok")
        return query

    def test_before_commit_is_held_before_backend_receives_it(self):
        backend, gate, client, _ = self.pair("before_commit")
        armed = self.arm(client)
        client.sendall(packet(b"\x03COMMIT"))
        self.assertTrue(gate.held.wait(3))
        self.assertEqual(backend.requests, [armed])
        self.assertFalse(gate.held_info["request_forwarded"])
        self.assertFalse(gate.held_info["server_ok_withheld"])
        client.settimeout(0.05)
        with self.assertRaises(socket.timeout):
            client.recv(1)
        gate.close()
        self.assertEqual(client.recv(1), b"")
        self.assertFalse(any(t.is_alive() for t in gate._threads))

    def test_after_commit_requires_backend_ok_but_client_cannot_receive_it(self):
        backend, gate, client, _ = self.pair("after_commit")
        self.arm(client)
        client.sendall(packet(b"\x03COMMIT"))
        self.assertTrue(gate.held.wait(3))
        self.assertEqual(backend.requests[-1], "COMMIT")
        self.assertTrue(gate.held_info["request_forwarded"])
        self.assertTrue(gate.held_info["server_ok_withheld"])
        client.settimeout(0.05)
        with self.assertRaises(socket.timeout):
            client.recv(1)

    def test_snapshot_barrier_never_logs_authentication(self):
        backend, gate, client, path = self.pair("snapshot")
        query = "CREATE SNAPSHOT `fixed` FOR DATABASE `source`"
        client.sendall(packet(b"\x03" + query.encode()))
        self.assertTrue(gate.held.wait(3))
        self.assertEqual(backend.requests, [query])
        self.assertEqual(gate.queries, [query])
        self.assertNotIn("private_auth_bytes", path.read_text())
        self.assertTrue(gate.held_info["server_ok_withheld"])

    def test_server_error_does_not_become_a_successful_commit_barrier(self):
        _, gate, client, _ = self.pair("after_commit", commit_error=True)
        self.arm(client)
        client.sendall(packet(b"\x03COMMIT"))
        self.assertEqual(read_packet(client)[4:], b"\xfferror")
        self.assertFalse(gate.held.is_set())

    def test_fragmented_header_and_payload_are_reassembled(self):
        data = bytearray(packet(b"\x03SELECT 1", 0))
        class Fragmented:
            def recv(self, size):
                count = min(size, 1, len(data))
                result = bytes(data[:count])
                del data[:count]
                return result
        self.assertEqual(read_packet(Fragmented()), packet(b"\x03SELECT 1", 0))

    def test_oversized_header_is_rejected_before_payload_allocation(self):
        data = bytearray((1024 * 1024 + 1).to_bytes(3, "little") + b"\x00")
        class Header:
            def recv(self, size):
                result = bytes(data[:size])
                del data[:size]
                return result
        with self.assertRaises(ValueError):
            read_packet(Header())

    def test_client_exit_cannot_count_as_a_reached_barrier(self):
        _, gate, _, _ = self.pair("snapshot")
        class Exited:
            def poll(self):
                return 1
        with self.assertRaisesRegex(RuntimeError, "exited before"):
            gate.wait_held(Exited())


if __name__ == "__main__":
    unittest.main()
