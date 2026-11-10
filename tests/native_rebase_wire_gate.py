"""Bounded plaintext MySQL relay for exact native-Rebase failure barriers.

Fixture only: no credentials/auth packets are logged; no held request/response
is ever released to a killed client. This is not a production proxy/parser.
"""

import json
from pathlib import Path
import socket
import threading
import time


def read_packet(stream):
    def exact(size):
        result = bytearray()
        while len(result) < size:
            part = stream.recv(size - len(result))
            if not part:
                raise EOFError("peer closed")
            result.extend(part)
        return bytes(result)
    header = exact(4)
    size = int.from_bytes(header[:3], "little")
    if size > 1024 * 1024:
        raise ValueError("fixture packet exceeds one MiB")
    return header + exact(size)


class RebaseWireGate:
    def __init__(self, upstream_port, mode, log_path, *, snapshot_prefix="", success_prefix=""):
        if mode not in ("snapshot", "before_commit", "after_commit"):
            raise ValueError("invalid native-Rebase wire barrier")
        if type(upstream_port) is not int or not 0 < upstream_port < 65536:
            raise ValueError("invalid private upstream port")
        if (mode == "snapshot" and not snapshot_prefix) or (mode != "snapshot" and not success_prefix):
            raise ValueError("missing exact barrier query prefix")
        self.mode, self.upstream_port = mode, upstream_port
        self.snapshot_prefix, self.success_prefix = snapshot_prefix, success_prefix
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self.held = threading.Event()
        self.held_info = None
        self.queries = []
        self._threads, self._sockets = [], set()
        self._accepted = 0
        self._log = Path(log_path).open("x")
        self._listener = socket.socket()
        try:
            self._listener.bind(("127.0.0.1", 0))
            self._listener.listen(8)
            self._listener.settimeout(0.1)
            self.port = self._listener.getsockname()[1]
            self._start(self._accept)
        except BaseException:
            self._listener.close()
            self._log.close()
            raise

    def _record(self, event):
        with self._lock:
            self._log.write(json.dumps(event) + "\n")
            self._log.flush()

    def _start(self, action, *args):
        with self._lock:
            if self._closed.is_set():
                return False
            thread = threading.Thread(target=action, args=args, daemon=True)
            self._threads.append(thread)
            thread.start()
            return True

    def _accept(self):
        while not self._closed.is_set():
            try:
                client, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                self._accepted += 1
                rejected = self._closed.is_set() or self._accepted > 8
                if not rejected:
                    self._sockets.add(client)
            if rejected:
                client.close()
                continue
            if not self._start(self._connect, client):
                self._disconnect(client)

    def _connect(self, client):
        server = None
        try:
            server = socket.create_connection(("127.0.0.1", self.upstream_port), timeout=5)
            server.settimeout(None)
            with self._lock:
                if self._closed.is_set():
                    return
                self._sockets.add(server)
            state = {"authenticated": False, "armed": False, "response_target": None}
            self._start(self._client_to_server, client, server, state)
            self._start(self._server_to_client, server, client, state)
        except OSError:
            self._disconnect(client, server)
        finally:
            if self._closed.is_set():
                self._disconnect(client, server)

    def _hold(self, info):
        with self._lock:
            if self.held_info is not None:
                raise ValueError("multiple wire barriers reached")
            self.held_info = info
            self._log.write(json.dumps({"event": "held", **info}) + "\n")
            self._log.flush()
            self.held.set()
        self._closed.wait()

    def _client_to_server(self, client, server, state):
        try:
            while not self._closed.is_set():
                packet = read_packet(client)
                payload = packet[4:]
                with self._lock:
                    authenticated = state["authenticated"]
                if authenticated and payload[:1] == b"\x03":
                    query = payload[1:].decode("utf-8")
                    with self._lock:
                        if len(self.queries) >= 512:
                            raise ValueError("fixture query budget exceeded")
                        self.queries.append(query)
                    self._record({"event": "query", "sql": query})
                    with self._lock:
                        if query.startswith(self.success_prefix) and self.success_prefix and "'COMMITTED'" in query:
                            state["armed"] = True
                        commit = state["armed"] and query.strip().rstrip(";").upper() == "COMMIT"
                    if commit and self.mode == "before_commit":
                        self._hold({"mode": self.mode, "request_forwarded": False, "server_ok_withheld": False})
                        return
                    target = (self.mode == "snapshot" and query.startswith(self.snapshot_prefix)) or (commit and self.mode == "after_commit")
                    if target:
                        with self._lock:
                            state["response_target"] = self.mode
                server.sendall(packet)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            self._disconnect(client, server)

    def _server_to_client(self, server, client, state):
        try:
            while not self._closed.is_set():
                packet = read_packet(server)
                payload = packet[4:]
                with self._lock:
                    if not state["authenticated"] and packet[3] >= 2 and payload[:1] == b"\x00":
                        state["authenticated"] = True
                    target = state["response_target"]
                    if target:
                        state["response_target"] = None
                if target and payload[:1] == b"\x00":
                    self._hold({"mode": target, "request_forwarded": True, "server_ok_withheld": True})
                    return
                client.sendall(packet)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            self._disconnect(client, server)

    def wait_held(self, process, timeout=30):
        deadline = time.monotonic() + timeout
        while not self.held.wait(0.02):
            if process.poll() is not None:
                raise RuntimeError("adapter exited before the wire barrier")
            if time.monotonic() >= deadline:
                raise TimeoutError("native-Rebase wire barrier not reached")
        return dict(self.held_info)

    def _disconnect(self, *sockets):
        for stream in sockets:
            if stream is None:
                continue
            try:
                stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            stream.close()
            with self._lock:
                self._sockets.discard(stream)

    def close(self):
        self._closed.set()
        try:
            self._listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._listener.close()
        with self._lock:
            streams = list(self._sockets)
        self._disconnect(*streams)
        deadline = time.monotonic() + 10
        for thread in list(self._threads):
            thread.join(max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in self._threads):
            raise RuntimeError("owned relay thread did not stop")
        self._log.close()
