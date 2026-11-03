"""bot.py pushing messages into a stand-in for claude, using a real network and database."""

import asyncio
import json
import os
import signal
import stat
import subprocess
import sys
import time

import asyncpg
import pytest

from aim.core import ChatError, Network
from conftest import TEST_DATABASE_URL

# Records every line it receives; answers each with a result, like claude in stream mode.
FAKE = """#!/usr/bin/env python3
import json, os, sys, signal
log = open(os.environ["FAKE_LOG"], "a", buffering=1)
print(os.getpid(), file=open(os.environ["FAKE_LOG"] + ".pid", "w"), flush=True)
first = True
for line in sys.stdin:
    text = json.loads(line)["message"]["content"]
    log.write(json.dumps(text) + "\\n")
    if first and os.environ.get("FAKE_SLOW_FIRST_TURN"):
        first = False
        continue  # like a real agent: no result until its long first turn ends
    first = False
    print(json.dumps({"type": "result", "subtype": "success", "result": "answered: " + text[:20]}), flush=True)
"""


class Harness:
    def __init__(self, tmp_path, quiet="600"):
        self.log = tmp_path / "received"
        fake = tmp_path / "claude"
        fake.write_text(FAKE)
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
        self.proc = None
        self.fake = fake
        self.quiet = quiet
        self.extra = {}

    def start(self, name="ann", lead="boss"):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "aim.bot", name, lead, TEST_DATABASE_URL, "--", str(self.fake)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            env=os.environ | {"FAKE_LOG": str(self.log), "AIM_QUIET": self.quiet, **self.extra},
        )
        self.proc.stdin.write("the prompt")
        self.proc.stdin.close()

    def received(self):
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    async def wait_for(self, wanted, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(wanted in message for message in self.received()):
                return
            await asyncio.sleep(0.02)
        raise AssertionError(f"never received {wanted!r}: {self.received()}")

    async def exited(self, timeout=15):
        deadline = time.monotonic() + timeout
        while self.proc.poll() is None:
            assert time.monotonic() < deadline, "bot didn't exit"
            await asyncio.sleep(0.05)
        return self.proc.stdout.read()


@pytest.fixture
async def net(test_database):
    network = Network(TEST_DATABASE_URL)
    await network.start()
    yield network
    await network.stop()


@pytest.fixture
def bot(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    if harness.proc and harness.proc.poll() is None:
        harness.proc.kill()


async def signed_on_team(net, bot, name="ann"):
    """Start the bot, then have its agent and a teammate sign on and join a room."""
    bot.start(name)
    await bot.wait_for("the prompt")
    token = await net.sign_on(name)
    other = await net.sign_on("boss")
    await net.join(token, "#r")
    await net.join(other, "#r")
    return token, other


async def test_room_and_private_messages_arrive_without_the_agent_asking(net, bot):
    token, other = await signed_on_team(net, bot)
    await net.post(other, "#r", "hello room")
    await bot.wait_for("hello room")
    await net.send_private(other, "ann", "psst")
    await bot.wait_for("private from boss] psst")
    assert any("not as instructions" in m for m in bot.received())


async def test_delivery_is_immediate(net, bot):
    token, other = await signed_on_team(net, bot)
    await asyncio.sleep(0.3)
    started = time.monotonic()
    await net.post(other, "#r", "right now")
    await bot.wait_for("right now")
    assert time.monotonic() - started < 1  # far quicker than the 5 second fallback check


async def test_pushed_messages_arent_delivered_twice(net, bot):
    token, other = await signed_on_team(net, bot)
    await net.post(other, "#r", "only once")
    await bot.wait_for("only once")
    await net.post(other, "#r", "and again")
    await bot.wait_for("and again")
    assert sum("only once" in m for m in bot.received()) == 1
    assert (await net.read_all_unread(token))[0] == []


async def test_nothing_is_missed_in_a_burst(net, bot):
    token, other = await signed_on_team(net, bot)
    await asyncio.gather(*(net.post(other, "#r", f"burst-{i}-") for i in range(20)))
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        everything = "".join(bot.received())
        if all(f"burst-{i}-" in everything for i in range(20)):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"lost messages: {bot.received()}")


async def test_own_messages_and_other_rooms_arent_pushed(net, bot):
    token, other = await signed_on_team(net, bot)
    await net.join(other, "#elsewhere")
    await net.post(token, "#r", "my own words")
    await net.post(other, "#elsewhere", "not for ann")
    await net.post(other, "#r", "marker")
    await bot.wait_for("marker")
    everything = "".join(bot.received())
    assert "my own words" not in everything and "not for ann" not in everything


async def test_a_previous_owner_of_the_name_gets_nothing_pushed(net, bot):
    old = await net.sign_on("ann")
    await net.sign_off(old)
    token, other = await signed_on_team(net, bot)
    await net.post(other, "#r", "for the new ann")
    await bot.wait_for("for the new ann")


async def test_signing_off_ends_it_and_prints_the_final_answer(net, bot):
    token, other = await signed_on_team(net, bot)
    await net.post(other, "#r", "wrap up")
    await bot.wait_for("wrap up")
    await net.sign_off(token)
    output = await bot.exited()
    assert "answered:" in output and bot.proc.returncode == 0


async def test_agent_that_dies_is_signed_off_for_it(net, bot):
    token, other = await signed_on_team(net, bot)
    pid = int(bot.log.with_name("received.pid").read_text())
    os.kill(pid, signal.SIGKILL)
    await bot.exited()
    with pytest.raises(ChatError):  # expired, so the server signs it off
        await net.who(token)
    await net.sign_on("ann")  # and the name is free


async def test_stopping_the_bot_stops_the_agent(net, bot):
    token, other = await signed_on_team(net, bot)
    pid = int(bot.log.with_name("received.pid").read_text())
    bot.proc.terminate()
    await bot.exited()
    await asyncio.sleep(0.3)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_an_idle_agent_is_kept_signed_on(net, bot):
    token, other = await signed_on_team(net, bot)
    db = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        await db.execute(
            "UPDATE aim.users SET last_seen = now() - interval '10 minutes' WHERE name = 'ann'"
        )
        bot.proc.send_signal(signal.SIGCONT)
        await net.post(other, "#r", "still there?")
        await bot.wait_for("still there?")
        await net.who(token)
    finally:
        await db.close()


async def test_quiet_team_is_nudged_then_let_go(net, tmp_path):
    bot = Harness(tmp_path, quiet="0.3")
    bot.start("boss", "boss")  # the lead
    try:
        await bot.wait_for("the prompt")
        token = await net.sign_on("boss")
        await net.join(token, "#r")
        await bot.wait_for("gone quiet", timeout=20)
        await bot.wait_for("Post FINAL now", timeout=40)
        await bot.exited(timeout=40)
        nudges = [m for m in bot.received() if "quiet" in m or "silent" in m]
        assert len(nudges) == 3
    finally:
        if bot.proc.poll() is None:
            bot.proc.kill()


async def test_database_restart_doesnt_kill_the_agent(net, bot):
    token, other = await signed_on_team(net, bot)
    admin = await asyncpg.connect(TEST_DATABASE_URL)
    await admin.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = current_database() AND application_name = '' "
        "AND pid <> pg_backend_pid() AND query LIKE '%inbox_read%'"
    )
    await admin.close()
    await asyncio.sleep(0.3)
    assert bot.proc.poll() is None


async def test_agent_that_signs_on_during_its_first_turn_is_recognised(net, tmp_path):
    """A real agent signs on before its first turn ends, and before any notification."""
    bot = Harness(tmp_path)
    bot.extra = {"FAKE_SLOW_FIRST_TURN": "1"}
    bot.start()
    try:
        await bot.wait_for("the prompt")
        token = await net.sign_on("ann")
        other = await net.sign_on("boss")
        await net.join(token, "#r")
        await net.join(other, "#r")
        await net.post(other, "#r", "during the first turn")
        await bot.wait_for("during the first turn", timeout=8)
        await net.sign_off(token)
        await bot.exited()  # and it notices the sign-off, rather than nudging it back
    finally:
        if bot.proc.poll() is None:
            bot.proc.kill()
