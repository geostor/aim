"""End to end over real HTTP: separate MCP clients chatting through a live server."""

import asyncio
import os
import socket
import subprocess
import sys
import time

import asyncpg
import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from conftest import TEST_DATABASE_URL


class Server:
    """The real server command, running as its own process on the test database."""

    def __init__(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/mcp"
        self.start()

    def start(self):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "aim.server", "--port", str(self.port)],
            env=os.environ | {"DATABASE_URL": TEST_DATABASE_URL},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 15
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), 0.2).close()
                return
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.1)

    def stop(self):
        self.proc.terminate()
        self.proc.wait()


@pytest.fixture(scope="module")
def server(test_database):
    running = Server()
    yield running
    running.stop()


@pytest.fixture(scope="module")
def url(server):
    return server.url


async def call(client, tool, **args):
    return (await client.call_tool(tool, args)).data


async def test_two_clients_chat(url):
    async with Client(url) as a, Client(url) as b:
        alice = (await call(a, "sign_on", screen_name="alice", profile="test agent"))["token"]
        bob = (await call(b, "sign_on", screen_name="bob"))["token"]

        await call(a, "join_room", token=alice, room="lobby")
        joined = await call(b, "join_room", token=bob, room="#lobby")
        assert joined["members"] == ["alice", "bob"]
        assert joined["op"] == "alice"

        # Bob blocks; Alice's post should wake him well before the timeout.
        started = time.monotonic()
        waiter = asyncio.create_task(call(b, "wait_for_messages", token=bob, timeout_seconds=20))
        await asyncio.sleep(0.2)
        await call(a, "post_message", token=alice, room="lobby", text="hello bob")
        got = await asyncio.wait_for(waiter, 5)
        assert time.monotonic() - started < 5
        assert [(m["from"], m["room"], m["text"]) for m in got["messages"]] == [
            ("alice", "#lobby", "hello bob")
        ]

        await call(b, "set_away", token=bob, message="thinking")
        sent = await call(a, "send_private_message", token=alice, to="bob", text="psst")
        assert sent["their_away_message"] == "thinking"
        private = await call(b, "get_private_messages", token=bob)
        assert private["messages"][0]["text"] == "psst"

        info = await call(b, "whois", token=bob, screen_name="alice")
        assert info["profile"] == "test agent" and info["rooms"] == ["#lobby"]

        await call(a, "sign_off", token=alice)
        await call(b, "sign_off", token=bob)


async def test_token_is_the_identity_across_connections(url):
    async with Client(url) as first:
        token = (await call(first, "sign_on", screen_name="carol"))["token"]
        await call(first, "join_room", token=token, room="persist")
    async with Client(url) as second:
        rooms = (await call(second, "whois", token=token, screen_name="carol"))["rooms"]
        assert rooms == ["#persist"]
        await call(second, "sign_off", token=token)


async def test_errors_reach_the_agent_readably(url):
    async with Client(url) as c:
        with pytest.raises(ToolError, match="Call sign_on"):
            await call(c, "list_rooms", token="bogus")
        token = (await call(c, "sign_on", screen_name="dave"))["token"]
        with pytest.raises(ToolError, match="No such room"):
            await call(c, "post_message", token=token, room="nowhere", text="hi")
        await call(c, "sign_off", token=token)


async def test_tool_docs_published(url):
    async with Client(url) as c:
        tools = {t.name: t for t in await c.list_tools()}
        assert len(tools) == 15
        assert all(t.description for t in tools.values())


async def test_a_token_is_required_to_act(url):
    async with Client(url) as c:
        with pytest.raises(ToolError):
            await call(c, "post_message", token="", room="x", text="hi")


async def test_restarting_the_server_wipes_it(server, url):
    async with Client(url) as c:
        token = (await call(c, "sign_on", screen_name="erin"))["token"]
        await call(c, "join_room", token=token, room="doomed")
        await call(c, "post_message", token=token, room="doomed", text="remember me")

    db = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        count = "SELECT count(*) FROM aim.events WHERE kind = 'message' AND sender = 'erin'"
        assert await db.fetchval(count) == 1
        server.stop()
        server.start()
        assert await db.fetchval(count) == 0
    finally:
        await db.close()

    async with Client(url) as c:
        with pytest.raises(ToolError, match="restarted"):
            await call(c, "list_rooms", token=token)
        token = (await call(c, "sign_on", screen_name="erin"))["token"]
        assert await call(c, "list_rooms", token=token) == {"rooms": []}
        await call(c, "sign_off", token=token)


async def test_join_replays_scrollback_over_mcp(url):
    async with Client(url) as a, Client(url) as b:
        frank = (await call(a, "sign_on", screen_name="frank"))["token"]
        await call(a, "join_room", token=frank, room="replay")
        await call(a, "post_message", token=frank, room="replay", text="before you got here")
        gina = (await call(b, "sign_on", screen_name="gina"))["token"]
        joined = await call(b, "join_room", token=gina, room="replay")
        assert [(m["from"], m["text"]) for m in joined["scrollback"]] == [
            ("frank", "before you got here")
        ]
        await call(a, "sign_off", token=frank)
        await call(b, "sign_off", token=gina)
