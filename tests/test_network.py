"""Chat rules, tested directly against the network on Postgres with a fake clock."""

import asyncio

import asyncpg
import pytest

from aim.core import IDLE_TIMEOUT, RATE_LIMIT, RATE_WINDOW, SCROLLBACK, ChatError, Network

from conftest import TEST_DATABASE_URL


class Clock:
    now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
async def net(clock, test_database):
    network = Network(TEST_DATABASE_URL, clock)
    await network.start()
    yield network
    await network.stop()


def texts(events):
    return [e.text for e in events if e.kind == "message"]


async def test_screen_names_are_unique_ignoring_case(net):
    await net.sign_on("Alice")
    with pytest.raises(ChatError, match="already signed on"):
        await net.sign_on("alice")


async def test_invalid_names_rejected(net):
    with pytest.raises(ChatError):
        await net.sign_on("has space")
    alice = await net.sign_on("alice")
    with pytest.raises(ChatError):
        await net.join(alice, "#bad room")


async def test_bad_token_rejected(net):
    with pytest.raises(ChatError, match="token"):
        await net.who("nope")


async def test_room_names_normalized(net):
    alice = await net.sign_on("alice")
    assert (await net.join(alice, "General"))["room"] == "#general"


async def test_unread_tracks_position_and_skips_own_messages(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    await net.post(alice, "#r", "one")
    await net.post(bob, "#r", "mine")
    assert texts((await net.read_room(bob, "#r"))[0]) == ["one"]
    assert (await net.read_room(bob, "#r"))[0] == []


async def test_unread_starts_at_join_but_history_is_available(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.post(alice, "#r", "before")
    await net.join(bob, "#r")
    assert (await net.read_room(bob, "#r"))[0] == []
    assert texts((await net.read_room(bob, "#r", since=0))[0]) == ["before"]


async def test_reading_since_does_not_move_unread(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    await net.post(alice, "#r", "hi")
    await net.read_room(bob, "#r", since=0)
    assert texts((await net.read_room(bob, "#r"))[0]) == ["hi"]


async def test_paging(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    for t in "abc":
        await net.post(alice, "#r", t)
    first, more = await net.read_room(bob, "#r", limit=2)
    assert texts(first) == ["a", "b"] and more
    rest, more = await net.read_room(bob, "#r", limit=2)
    assert texts(rest) == ["c"] and not more


async def test_must_join_to_read_or_post(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    with pytest.raises(ChatError, match="not in"):
        await net.post(bob, "#r", "hi")
    with pytest.raises(ChatError, match="not in"):
        await net.read_room(bob, "#r")


async def test_private_messages(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.send_private(alice, "BOB", "psst")
    assert texts((await net.read_private(bob))[0]) == ["psst"]
    assert (await net.read_private(bob))[0] == []
    assert (await net.read_private(alice))[0] == []  # own messages aren't unread
    assert texts((await net.read_private(alice, since=0))[0]) == ["psst"]


async def test_private_message_to_offline_user_fails(net):
    alice = await net.sign_on("alice")
    with pytest.raises(ChatError, match="not signed on"):
        await net.send_private(alice, "ghost", "hello?")


async def test_only_op_can_set_topic_or_kick(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    with pytest.raises(ChatError, match="operator"):
        await net.set_topic(bob, "#r", "mine now")
    with pytest.raises(ChatError, match="operator"):
        await net.kick(bob, "#r", "alice")
    await net.set_topic(alice, "#r", "ok")
    assert (await net.list_rooms(alice))[0]["topic"] == "ok"


async def test_kicked_user_is_removed_and_notified(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    await net.kick(alice, "#r", "bob", "spam")
    with pytest.raises(ChatError, match="not in"):
        await net.post(bob, "#r", "hey")
    notice = (await net.read_all_unread(bob))[0]
    assert [(e.kind, e.room, e.text) for e in notice] == [("kick", "#r", "spam")]
    assert (await net.read_all_unread(bob))[0] == []


async def test_bystander_reads_kick_event_only_once(net):
    alice, bob, carol = await net.sign_on("alice"), await net.sign_on("bob"), await net.sign_on("carol")
    for user in (alice, bob, carol):
        await net.join(user, "#r")
    await net.read_all_unread(carol)
    await net.kick(alice, "#r", "bob")
    assert [e.kind for e in (await net.read_all_unread(carol))[0]] == ["kick"]
    assert (await net.read_all_unread(carol))[0] == []


async def test_op_passes_on_and_empty_rooms_vanish(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    await net.leave(alice, "#r")
    await net.set_topic(bob, "#r", "bob runs this now")
    await net.leave(bob, "#r")
    assert await net.list_rooms(await net.sign_on("carol")) == []


async def test_idle_users_time_out_and_free_their_name(net, clock):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    clock.now += IDLE_TIMEOUT - 1
    await net.who(bob)
    clock.now += 2
    await net.who(bob)
    with pytest.raises(ChatError):
        await net.who(alice)
    assert [e.kind for e in (await net.read_room(bob, "#r"))[0]] == ["quit"]
    await net.sign_on("alice")


async def test_rate_limit_then_recovery(net, clock):
    alice = await net.sign_on("alice")
    await net.join(alice, "#r")
    for _ in range(RATE_LIMIT):
        await net.post(alice, "#r", "x")
    with pytest.raises(ChatError, match="Rate limit"):
        await net.post(alice, "#r", "x")
    clock.now += RATE_WINDOW
    await net.post(alice, "#r", "x")


async def test_read_all_unread_merges_rooms_and_private(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#a")
    await net.join(alice, "#b")
    await net.join(bob, "#a")
    await net.join(bob, "#b")
    await net.post(alice, "#b", "1")
    await net.send_private(alice, "bob", "2")
    await net.post(alice, "#a", "3")
    assert texts((await net.read_all_unread(bob))[0]) == ["1", "2", "3"]
    assert (await net.read_all_unread(bob))[0] == []


async def test_read_all_unread_paging_loses_nothing(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#a")
    await net.join(bob, "#a")
    await net.post(alice, "#a", "1")
    await net.send_private(alice, "bob", "2")
    await net.post(alice, "#a", "3")
    first, more = await net.read_all_unread(bob, limit=2)
    assert texts(first) == ["1", "2"] and more
    assert texts((await net.read_all_unread(bob, limit=2))[0]) == ["3"]


async def test_wait_returns_when_message_arrives(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    waiter = asyncio.create_task(net.wait_for_messages(bob, 5))
    await asyncio.sleep(0.01)
    assert not waiter.done()
    await net.post(alice, "#r", "wake up")
    events, _ = await asyncio.wait_for(waiter, 1)
    assert texts(events) == ["wake up"]


async def test_wait_times_out_empty(net):
    bob = await net.sign_on("bob")
    assert await net.wait_for_messages(bob, 0.01) == ([], False)


async def test_wait_fails_if_signed_off_meanwhile(net):
    bob = await net.sign_on("bob")
    waiter = asyncio.create_task(net.wait_for_messages(bob, 5))
    await asyncio.sleep(0.01)
    await net.sign_off(bob)
    with pytest.raises(ChatError, match="token"):
        await asyncio.wait_for(waiter, 1)


async def test_second_message_during_wait_is_not_lost(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    received = []
    async def listen():
        while len(received) < 20:
            received.extend(texts((await net.wait_for_messages(bob, 5))[0]))
    listener = asyncio.create_task(listen())
    await asyncio.gather(*(net.post(alice, "#r", str(i)) for i in range(20)))
    await asyncio.wait_for(listener, 10)
    assert sorted(map(int, received)) == list(range(20))


async def test_new_owner_of_a_screen_name_cannot_read_old_private_messages(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.send_private(bob, "alice", "secret")
    await net.sign_off(alice)
    impostor = await net.sign_on("alice")
    assert (await net.read_private(impostor, since=0))[0] == []
    assert (await net.read_all_unread(impostor))[0] == []


async def test_old_token_stops_working_after_sign_off(net):
    alice = await net.sign_on("alice")
    await net.sign_off(alice)
    with pytest.raises(ChatError, match="token"):
        await net.who(alice)


async def test_room_history_outlives_the_room(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.post(alice, "#r", "for the record")
    await net.leave(alice, "#r")
    await net.join(bob, "#r")
    assert texts((await net.read_room(bob, "#r", since=0))[0]) == ["for the record"]


async def test_closed_room_reopens_fresh(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.set_topic(alice, "#r", "old")
    await net.leave(alice, "#r")
    info = await net.join(bob, "#r")
    assert info["topic"] == "" and info["op"] == "bob"


async def test_text_postgres_cannot_store_is_cleaned(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(bob, "#r")
    await net.post(alice, "#r", "nul\x00 and \ud800 surrogate")
    assert texts((await net.read_room(bob, "#r"))[0]) == ["nul and ? surrogate"]


async def test_failed_call_changes_nothing(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    with pytest.raises(ChatError):
        await net.set_topic(bob, "#r", "nope")
    assert (await net.list_rooms(alice))[0]["topic"] == ""


# --- the database itself ------------------------------------------------------


async def test_conversation_is_stored_in_postgres(net):
    alice = await net.sign_on("alice", "a profile")
    await net.join(alice, "#r")
    await net.post(alice, "#r", "hello")
    db = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        row = await db.fetchrow(
            "SELECT sender, room, text FROM aim.events WHERE kind = 'message'"
        )
        user = await db.fetchrow("SELECT name, token_hash FROM aim.users")
    finally:
        await db.close()
    assert tuple(row) == ("alice", "#r", "hello")
    assert user["name"] == "alice" and user["token_hash"] != alice


async def test_restart_wipes_everything(net, clock):
    alice = await net.sign_on("alice")
    await net.join(alice, "#r")
    await net.post(alice, "#r", "gone soon")
    await net.stop()
    await net.start()
    with pytest.raises(ChatError, match="restarted"):
        await net.who(alice)
    alice = await net.sign_on("alice")  # the name is free again
    assert await net.list_rooms(alice) == []
    db = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        assert await db.fetchval("SELECT count(*) FROM aim.events") == 0
    finally:
        await db.close()


async def test_wipe_leaves_other_data_in_the_database_alone(net):
    db = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        await db.execute(
            "DROP TABLE IF EXISTS public.bystander; CREATE TABLE public.bystander (x int);"
            "INSERT INTO public.bystander VALUES (1);"
        )
        await net.stop()
        await net.start()
        assert await db.fetchval("SELECT count(*) FROM public.bystander") == 1
    finally:
        await db.execute("DROP TABLE IF EXISTS public.bystander")
        await db.close()


async def test_second_server_refuses_instead_of_wiping_a_live_one(net):
    alice = await net.sign_on("alice")
    second = Network(TEST_DATABASE_URL)
    with pytest.raises(RuntimeError, match="Another"):
        await second.start()
    await net.who(alice)


async def test_unreachable_postgres_gives_a_helpful_error():
    with pytest.raises(RuntimeError, match="Can't reach Postgres"):
        await Network("postgresql://aim:aim@127.0.0.1:1/aim").start()


async def test_join_replays_last_messages_oldest_first(net, clock):
    alice = await net.sign_on("alice")
    await net.join(alice, "#r")
    for i in range(SCROLLBACK + 5):
        clock.now += RATE_WINDOW / RATE_LIMIT  # stay under the rate limit
        await net.post(alice, "#r", str(i))
    bob = await net.sign_on("bob")
    info = await net.join(bob, "#r")
    assert [m["text"] for m in info["scrollback"]] == [str(i) for i in range(5, SCROLLBACK + 5)]
    assert {m["kind"] for m in info["scrollback"]} == {"message"}


async def test_scrollback_is_not_delivered_again_as_unread(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.post(alice, "#r", "earlier")
    info = await net.join(bob, "#r")
    assert [m["text"] for m in info["scrollback"]] == ["earlier"]
    assert (await net.read_all_unread(bob))[0] == []
    await net.post(alice, "#r", "later")
    assert texts((await net.read_all_unread(bob))[0]) == ["later"]


async def test_scrollback_leaves_out_private_messages_and_other_rooms(net):
    alice, bob = await net.sign_on("alice"), await net.sign_on("bob")
    await net.join(alice, "#r")
    await net.join(alice, "#other")
    await net.post(alice, "#other", "elsewhere")
    await net.send_private(alice, "bob", "secret")
    assert (await net.join(bob, "#r"))["scrollback"] == []
