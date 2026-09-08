"""The download, against a server that behaves like a bad connection.

The docstring in `_fetch` describes two truncation modes reproduced against a
local server. This is that server, kept, so the claims stay checkable and the
resume path is exercised by something other than a real flaky mirror.

Every test here drives real sockets through `urllib`. Nothing is monkeypatched
inside `_fetch`, because the behaviour under test is precisely how it reacts to
what a server does -- a stub would be asserting the mock.
"""

import hashlib
import http.server
import threading

import pytest

from scripts import get_data

BODY = b"".join(f"row {i:06d},value\n".encode() for i in range(4000))
DIGEST = hashlib.sha256(BODY).hexdigest()


@pytest.fixture(autouse=True)
def _pin_the_expected_file(monkeypatch):
    monkeypatch.setattr(get_data, "EXPECTED_BYTES", len(BODY))
    monkeypatch.setattr(get_data, "EXPECTED_SHA256", DIGEST)
    # The retry sleeps between attempts; nothing here is waiting on a real
    # network. Patched by name rather than through the module attribute, so
    # this fixture does not itself depend on how get_data imports time.
    monkeypatch.setattr("time.sleep", lambda _: None)


class _Handler(http.server.BaseHTTPRequestHandler):
    """Configured per-test through class attributes on a subclass."""

    drops_remaining = 0  # how many more requests close early
    honours_range = True
    always_errors = False
    cut_at = len(BODY) // 3

    def log_message(self, *_args):  # keep pytest output readable
        pass

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's interface
        cls = type(self)
        if cls.always_errors:
            self.send_error(503, "mirror unavailable")
            return
        start = 0
        header = self.headers.get("Range")
        if header and cls.honours_range:
            start = int(header.removeprefix("bytes=").rstrip("-"))
            self.send_response(206)
            self.send_header(
                "Content-Range", f"bytes {start}-{len(BODY) - 1}/{len(BODY)}"
            )
        else:
            self.send_response(200)

        payload = BODY[start:]
        if cls.drops_remaining > 0:
            cls.drops_remaining -= 1
            # An honest Content-Length followed by a short body: the mode the
            # real mirror exhibits, and the one that raises rather than
            # returning a silently truncated file.
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload[: cls.cut_at])
            self.close_connection = True
            return

        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _serve(handler_cls):
    server = http.server.HTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_port}/file.csv"


@pytest.fixture
def server(request):
    attributes = getattr(request, "param", {})
    handler = type("Configured", (_Handler,), dict(attributes))
    srv, url = _serve(handler)
    yield url, handler
    srv.shutdown()
    srv.server_close()


@pytest.mark.parametrize("server", [{"drops_remaining": 0}], indirect=True)
def test_a_clean_download_lands_and_leaves_no_sidecar(server, tmp_path):
    url, _ = server
    dest = tmp_path / "online_retail.csv"
    get_data._fetch(url, dest)
    assert dest.read_bytes() == BODY
    assert not dest.with_name(dest.name + ".part").exists()


@pytest.mark.parametrize("server", [{"drops_remaining": 2}], indirect=True)
def test_a_connection_that_drops_twice_is_resumed_not_restarted(server, tmp_path):
    """The point of the Range header: a drop costs the remainder, not the file."""
    url, handler = server
    dest = tmp_path / "online_retail.csv"
    get_data._fetch(url, dest)
    assert dest.read_bytes() == BODY
    assert handler.drops_remaining == 0, "the server did not drop as configured"


@pytest.mark.parametrize(
    "server", [{"drops_remaining": 2, "honours_range": False}], indirect=True
)
def test_a_server_that_ignores_range_is_restarted_not_appended_to(server, tmp_path):
    """A 200 in reply to a Range request means "here is the whole thing".

    Appending it to a partial file yields a longer-than-expected file of
    interleaved garbage. This is the case where a naive resume corrupts data
    rather than failing, so the size and digest have to come out right.
    """
    url, _ = server
    dest = tmp_path / "online_retail.csv"
    get_data._fetch(url, dest)
    assert dest.read_bytes() == BODY


@pytest.mark.parametrize("server", [{"drops_remaining": 99}], indirect=True)
def test_a_download_that_never_completes_publishes_nothing(server, tmp_path):
    """Exhausted attempts must leave the destination absent and the sidecar
    gone -- a partial file that survives is one the next run trusts.

    This exits through the size gate rather than the retry's own error: a body
    cut short after an honest Content-Length does not always raise, so the
    attempts run out quietly and the file is simply too small. Both routes end
    the same way, and the message says so, which is what this asserts.
    """
    url, _ = server
    dest = tmp_path / "online_retail.csv"
    with pytest.raises(OSError, match="nothing was published"):
        get_data._fetch(url, dest, attempts=3)
    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()


@pytest.mark.parametrize("server", [{"always_errors": True}], indirect=True)
def test_a_server_that_only_errors_gives_up_and_says_how_far_it_got(server, tmp_path):
    """The other exhaustion route: every attempt raises rather than returning
    a short file."""
    url, _ = server
    dest = tmp_path / "online_retail.csv"
    with pytest.raises(OSError, match="3 attempts failed"):
        get_data._fetch(url, dest, attempts=3)
    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()


@pytest.mark.parametrize("server", [{"drops_remaining": 0}], indirect=True)
def test_the_wrong_file_is_rejected_even_though_it_arrived_intact(
    server, tmp_path, monkeypatch
):
    """The guard on the retry: resuming must not become a way to assemble
    something that is the right length and the wrong file."""
    url, _ = server
    monkeypatch.setattr(get_data, "EXPECTED_SHA256", "0" * 64)
    dest = tmp_path / "online_retail.csv"
    with pytest.raises(OSError, match="does not match the expected"):
        get_data._fetch(url, dest)
    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()
