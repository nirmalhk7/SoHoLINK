"""Regression tests for the standalone HTTP API client."""

from contextlib import redirect_stdout
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import socketserver
import threading
import time
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
import requests

import test_api as client


class LocalServer(ThreadingHTTPServer):
    def server_bind(self):
        # Avoid reverse DNS: this fixture only needs an ephemeral loopback port.
        socketserver.TCPServer.server_bind(self)


class SlowResponse(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply("challenge" if self.path.endswith("challenge") else "query")

    def do_POST(self):
        self.reply("connect")

    def reply(self, phase):
        if phase == self.server.slow_phase:
            time.sleep(0.25)
        payload = json.dumps({"nonce": "test-nonce", "token": "test-token"}).encode()
        self.send_response(200)
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            pass  # The timed-out client has already closed its connection.

    def log_message(self, *_args):
        pass  # Keep expected timeouts quiet.


class APIClientTests(unittest.TestCase):
    def test_each_request_has_finite_timeout(self):
        for phase in ("challenge", "connect", "query"):
            with self.subTest(phase=phase):
                self.check_timeout(phase)

    def check_timeout(self, phase):
        server = LocalServer(("127.0.0.1", 0), SlowResponse)
        server.slow_phase = phase
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        thread.start()
        try:
            base_url = f"http://127.0.0.1:{server.server_address[1]}"
            with tempfile.TemporaryDirectory() as directory:
                key_path = Path(directory) / "private.pem"
                key_path.write_bytes(ed25519.Ed25519PrivateKey.generate().private_bytes(
                    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ))
                with patch.object(client, "REQUEST_TIMEOUT", 0.05):
                    with redirect_stdout(io.StringIO()), self.assertRaises(requests.exceptions.Timeout):
                        if phase == "query":
                            client.query_api(base_url, "/protected", "test-token")
                        else:
                            client.authenticate(base_url, key_path)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
