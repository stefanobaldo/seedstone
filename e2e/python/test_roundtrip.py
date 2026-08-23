"""Drives a running seedstone with redis-py.

The library is doing the talking: it decides the handshake, it decides what a
reply means, and it raises if the server answers something it did not expect.
A test that built the frames itself would only prove this project agrees with
itself.
"""

import os
import socket

import pytest
import redis

PORT = int(os.environ.get("SEEDSTONE_PORT", "6390"))
# The lane's password, or nothing when the server was started open. A literal
# in CI that protects nothing: it exists so this lane exercises the
# authenticated path rather than the open one.
PASSWORD = os.environ.get("SEEDSTONE_PASSWORD")


KEYS = ("k", "k2", "n", "fresh", "brief", "missing")


@pytest.fixture(scope="module")
def r():
    client = redis.Redis(port=PORT, password=PASSWORD, decode_responses=True)
    # The server has no FLUSHALL and this gate does not need one, but a
    # counter that survives a re-run against a server someone left up would
    # fail the second run and pass the first. Clearing what these tests own is
    # the difference between a gate and a coin toss.
    client.delete(*KEYS)
    yield client
    client.close()


def test_roundtrip(r):
    assert r.ping() is True
    assert r.set("k", "v") is True
    assert r.get("k") == "v"
    assert r.set("k2", "v2", ex=100) is True
    assert 90 < r.ttl("k2") <= 100
    assert r.ttl("missing") == -2
    assert r.exists("k", "k2", "missing") == 2
    assert r.delete("k", "k2") == 2
    assert r.incrby("n", 5) == 5
    assert r.set("fresh", "a", nx=True) is True
    assert r.set("fresh", "b", nx=True) is None
    assert r.get("fresh") == "a"
    assert r.expire("fresh", 100) is True


def test_expiration_is_observed_by_the_client(r):
    """A deadline the client can outlive, so the expiry is seen and not assumed."""
    assert r.set("brief", "x", px=200) is True
    assert r.get("brief") == "x"
    # Slept rather than faked: this is the one clock in the gate that is real,
    # and the point of the e2e layer is that nothing here is simulated.
    import time

    time.sleep(0.5)
    assert r.get("brief") is None
    assert r.ttl("brief") == -2


def test_the_server_names_itself(r):
    """`HELLO` and `INFO` are what a client reads to decide what it reached."""
    # A flat array of alternating keys and values, which is how RESP2 carries
    # a map — redis-py hands it back unpaired, so the pairing is the assertion
    # that the shape is the one RESP3-less clients expect.
    flat = r.execute_command("HELLO")
    hello = dict(zip(flat[::2], flat[1::2]))
    assert hello["server"] == "seedstone"
    assert hello["proto"] == 2
    assert hello["mode"] == "standalone"
    info = r.info()
    assert info["redis_mode"] == "standalone"
    assert info["tcp_port"] == PORT
    assert info["connected_clients"] >= 1


@pytest.mark.skipif(not PASSWORD, reason="the server was started open")
def test_hello_without_credentials_is_refused():
    """A node with a password does not describe itself to a peer that has not
    given it, and the refusal names the form that would have worked.

    A socket rather than the library, which is the exception in this file and
    is why: redis-py 5 sends `CLIENT SETINFO` inside `connect()`, is refused
    for want of a password and raises there, so a credential-less `HELLO` is
    unreachable through its API. The bytes are what the server is being held
    to here, and they are the bytes any client would read.
    """
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=5)
    try:
        sock.sendall(b"*1\r\n$5\r\nHELLO\r\n*1\r\n$4\r\nPING\r\n")
        replies = b""
        while replies.count(b"\r\n") < 2:
            chunk = sock.recv(4096)
            assert chunk, "the server closed the connection"
            replies += chunk
        handshake, general = replies.split(b"\r\n")[:2]
        assert handshake.startswith(b"-NOAUTH HELLO must be called")
        assert b"HELLO AUTH <user> <pass>" in handshake
        # The general refusal is a different text, and a client tells the two
        # requests apart by them.
        assert general == b"-NOAUTH Authentication required."
    finally:
        sock.close()
