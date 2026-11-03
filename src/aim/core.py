"""The chat network itself: users, rooms and messages, stored in Postgres.

The database is scratch space for one run of the server: start() wipes it. Only
the dedicated SCHEMA is ever touched, so other data in the same database is safe.

All database work goes through a pool of one connection, so transactions run one
at a time. That rules out races between agents, and it keeps event ids in commit
order, which the unread tracking relies on: otherwise a reader could see event 6
before event 5 commits, move past 5, and never receive it.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
from fastmcp.exceptions import ToolError

SCHEMA = "aim"
INSTANCE_LOCK = 0x41494D  # advisory lock key held by the running server

MAX_TEXT = 4000
MAX_PROFILE = 500
MAX_SHORT = 300  # topics, kick reasons, part and quit messages
IDLE_TIMEOUT = 900.0  # seconds without a tool call before a user is signed off
MAX_WAIT = 60.0  # longest a single wait_for_messages call may block
RATE_LIMIT = 30  # messages per user...
RATE_WINDOW = 60.0  # ...per this many seconds
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
SCROLLBACK = 250  # past messages replayed to an agent when it joins a room

_SCREEN_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_-]{1,31}")
_ROOM_NAME = re.compile(r"#[a-z0-9][a-z0-9_-]{0,31}")

# Events name their room and users as plain text, so the conversation record
# outlives empty rooms. Signed-off users keep their row for the same reason, and
# a screen name is only unique among users who are still online.
_TABLES = """
CREATE TABLE users (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL,
    token_hash text UNIQUE,
    profile    text NOT NULL DEFAULT '',
    away       text,
    signed_on  timestamptz NOT NULL,
    last_seen  timestamptz NOT NULL,
    signed_off timestamptz,
    inbox_read bigint NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX users_online_name ON users (lower(name)) WHERE signed_off IS NULL;

CREATE TABLE rooms (
    name  text PRIMARY KEY,
    topic text NOT NULL DEFAULT '',
    op_id bigint REFERENCES users (id)
);

CREATE TABLE members (
    room      text NOT NULL REFERENCES rooms (name) ON DELETE CASCADE,
    user_id   bigint NOT NULL REFERENCES users (id),
    position  bigserial,
    last_read bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (room, user_id)
);

CREATE TABLE events (
    id           bigserial PRIMARY KEY,
    time         timestamptz NOT NULL,
    kind         text NOT NULL,
    sender_id    bigint NOT NULL REFERENCES users (id),
    sender       text NOT NULL,
    text         text NOT NULL DEFAULT '',
    room         text,
    recipient_id bigint REFERENCES users (id),
    recipient    text
);
CREATE INDEX events_room ON events (room, id);
CREATE INDEX events_recipient ON events (recipient_id, id);
CREATE INDEX events_sender ON events (sender_id, id);

-- Tells each agent's bot process the moment there's something new.
CREATE FUNCTION notify_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('aim_events', NEW.id::text);
    RETURN NULL;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER events_notify AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION notify_event();
"""


class ChatError(ToolError):
    """A request the network refuses. As a ToolError, its message reaches the agent."""


@dataclass(frozen=True)
class Event:
    id: int
    time: datetime
    kind: str  # message | join | part | quit | topic | kick
    sender: str
    text: str = ""
    room: str | None = None
    to: str | None = None

    def to_dict(self) -> dict:
        data = {
            "id": self.id,
            "time": self.time.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "kind": self.kind,
            "from": self.sender,
            "text": self.text,
        }
        if self.room:
            data["room"] = self.room
        if self.to:
            data["to"] = self.to
        return data


def _event(row: asyncpg.Record) -> Event:
    return Event(
        row["id"], row["time"], row["kind"], row["sender"], row["text"], row["room"], row["recipient"]
    )


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8", "replace")).hexdigest()


def _room_name(name: str) -> str:
    name = name.strip().lower()
    if not name.startswith("#"):
        name = "#" + name
    if not _ROOM_NAME.fullmatch(name):
        raise ChatError(
            f"Invalid room name {name!r}: use up to 32 letters, digits, '-' or '_'."
        )
    return name


def _text(text: str, limit: int = MAX_SHORT) -> str:
    """Tidy free text, and drop what Postgres can't store (NUL, lone surrogates)."""
    text = text.encode("utf-8", "replace").decode().replace("\x00", "").strip()
    if len(text) > limit:
        raise ChatError(f"Text is {len(text)} characters; the limit is {limit}.")
    return text


def _message(text: str) -> str:
    text = _text(text, MAX_TEXT)
    if not text:
        raise ChatError("Message is empty.")
    return text


def _page(rows: list[asyncpg.Record], limit: int) -> tuple[list[Event], bool]:
    return [_event(r) for r in rows[:limit]], len(rows) > limit


def _clamp_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


class Network:
    def __init__(self, dsn: str, clock: Callable[[], float] = time.time):
        self._dsn = dsn
        self._clock = clock
        self._pool: asyncpg.Pool | None = None
        self._instance_lock: asyncpg.Connection | None = None
        self._dirty = False
        self._changed = asyncio.Event()  # replaced each time it fires

    # --- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """Claim the database and wipe it, leaving empty tables for this run."""
        try:
            lock = await asyncpg.connect(self._dsn)
        except OSError as e:
            raise RuntimeError(
                f"Can't reach Postgres ({e}). Is it running? The README shows how to "
                "start it, and DATABASE_URL sets where to find it."
            ) from e
        if not await lock.fetchval("SELECT pg_try_advisory_lock($1::bigint)", INSTANCE_LOCK):
            await lock.close()
            raise RuntimeError(
                "Another Agentic Instant Messenger server is using this database. "
                "Starting would wipe its live data, so this one is stopping."
            )
        self._instance_lock = lock
        await lock.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA};")
        self._pool = await asyncpg.create_pool(
            self._dsn, min_size=1, max_size=1, server_settings={"search_path": SCHEMA}
        )
        await self._pool.execute(_TABLES)

    async def stop(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
        if self._instance_lock:
            await self._instance_lock.close()
            self._instance_lock = None

    @asynccontextmanager
    async def _tx(self):
        """A transaction, after signing off idle users. Waiters wake once it commits."""
        async with self._pool.acquire() as db:
            self._dirty = False
            async with db.transaction():
                await self._sweep(db)
                yield db
            if self._dirty:
                changed, self._changed = self._changed, asyncio.Event()
                changed.set()

    # --- identity -----------------------------------------------------------

    async def sign_on(self, screen_name: str, profile: str = "") -> str:
        """Create a user and return their token. Only its hash is stored."""
        if not _SCREEN_NAME.fullmatch(screen_name):
            raise ChatError(
                "Invalid screen name: use 2-32 letters, digits, '-' or '_', "
                "starting with a letter."
            )
        profile = _text(profile, MAX_PROFILE)
        token = secrets.token_urlsafe(24)
        now = self._now()
        async with self._tx() as db:
            taken = await db.fetchval(
                "SELECT 1 FROM users WHERE lower(name) = lower($1) AND signed_off IS NULL",
                screen_name,
            )
            if taken:
                raise ChatError(
                    f"{screen_name} is already signed on. Pick another name, or wait "
                    f"for it to expire after {int(IDLE_TIMEOUT)}s of inactivity."
                )
            await db.execute(
                "INSERT INTO users (name, token_hash, profile, signed_on, last_seen) "
                "VALUES ($1, $2, $3, $4, $4)",
                screen_name, _hash_token(token), profile, now,
            )
        return token

    async def sign_off(self, token: str, message: str = "") -> None:
        message = _text(message)
        async with self._tx() as db:
            await self._remove_user(db, await self._auth(db, token), message)

    async def set_away(self, token: str, message: str | None) -> str | None:
        message = _text(message, MAX_PROFILE) if message else None
        async with self._tx() as db:
            me = await self._auth(db, token)
            await db.execute("UPDATE users SET away = $2 WHERE id = $1", me["id"], message or None)
        return message or None

    async def whois(self, token: str, screen_name: str) -> dict:
        async with self._tx() as db:
            await self._auth(db, token)
            user = await self._find_user(db, screen_name)
            rooms = await db.fetch(
                "SELECT room FROM members WHERE user_id = $1 ORDER BY room", user["id"]
            )
            return self._describe(user) | {"rooms": [r["room"] for r in rooms]}

    async def who(self, token: str, room: str | None = None) -> list[dict]:
        async with self._tx() as db:
            await self._auth(db, token)
            if room is None:
                users = await db.fetch("SELECT * FROM users WHERE signed_off IS NULL ORDER BY id")
            else:
                found = await self._find_room(db, room)
                users = await db.fetch(
                    "SELECT u.* FROM users u JOIN members m ON m.user_id = u.id "
                    "WHERE m.room = $1 ORDER BY m.position",
                    found["name"],
                )
            return [self._describe(u) for u in users]

    # --- rooms --------------------------------------------------------------

    async def list_rooms(self, token: str) -> list[dict]:
        async with self._tx() as db:
            await self._auth(db, token)
            rows = await db.fetch(
                "SELECT r.name, r.topic, count(*) AS members FROM rooms r "
                "JOIN members m ON m.room = r.name GROUP BY r.name ORDER BY r.name"
            )
            return [{"room": r["name"], "topic": r["topic"], "members": r["members"]} for r in rows]

    async def join(self, token: str, room_name: str) -> dict:
        name = _room_name(room_name)
        async with self._tx() as db:
            me = await self._auth(db, token)
            await db.execute(
                "INSERT INTO rooms (name, op_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                name, me["id"],
            )
            joined = await db.fetchval(
                "INSERT INTO members (room, user_id) VALUES ($1, $2) "
                "ON CONFLICT DO NOTHING RETURNING 1",
                name, me["id"],
            )
            if joined:
                event = await self._add_event(db, "join", me, room=name)
                # Unread starts now; earlier history is available via `since`.
                await db.execute(
                    "UPDATE members SET last_read = $3 WHERE room = $1 AND user_id = $2",
                    name, me["id"], event.id,
                )
            return await self._room_info(db, name)

    async def leave(self, token: str, room_name: str, message: str = "") -> None:
        message = _text(message)
        async with self._tx() as db:
            me = await self._auth(db, token)
            room = await self._member_room(db, me, room_name)
            await self._add_event(db, "part", me, message, room=room["name"])
            await self._drop_member(db, room["name"], me["id"])

    async def set_topic(self, token: str, room_name: str, topic: str) -> str:
        topic = _text(topic)
        async with self._tx() as db:
            me = await self._auth(db, token)
            room = await self._member_room(db, me, room_name)
            self._require_op(me, room)
            await db.execute("UPDATE rooms SET topic = $2 WHERE name = $1", room["name"], topic)
            await self._add_event(db, "topic", me, topic, room=room["name"])
        return topic

    async def kick(self, token: str, room_name: str, target_name: str, reason: str = "") -> None:
        reason = _text(reason)
        async with self._tx() as db:
            me = await self._auth(db, token)
            room = await self._member_room(db, me, room_name)
            self._require_op(me, room)
            target = await db.fetchrow(
                "SELECT u.* FROM users u JOIN members m ON m.user_id = u.id "
                "WHERE m.room = $1 AND lower(u.name) = lower($2)",
                room["name"], target_name,
            )
            if target is None:
                raise ChatError(f"{target_name} is not in {room['name']}.")
            if target["id"] == me["id"]:
                raise ChatError("Use leave_room to leave a room.")
            text = f"{target['name']}: {reason}" if reason else target["name"]
            await self._add_event(db, "kick", me, text, room=room["name"])
            await self._drop_member(db, room["name"], target["id"])
            # They can no longer read the room, so tell them privately.
            await self._add_event(db, "kick", me, reason, room=room["name"], recipient=target)

    # --- messages -----------------------------------------------------------

    async def post(self, token: str, room_name: str, text: str) -> Event:
        text = _message(text)
        async with self._tx() as db:
            me = await self._auth(db, token)
            room = await self._member_room(db, me, room_name)
            await self._check_rate(db, me)
            return await self._add_event(db, "message", me, text, room=room["name"])

    async def send_private(self, token: str, target_name: str, text: str) -> tuple[Event, str | None]:
        """Send a private message; returns it with the recipient's away message."""
        text = _message(text)
        async with self._tx() as db:
            me = await self._auth(db, token)
            target = await self._find_user(db, target_name)
            if target["id"] == me["id"]:
                raise ChatError("You can't send a private message to yourself.")
            await self._check_rate(db, me)
            event = await self._add_event(db, "message", me, text, recipient=target)
            return event, target["away"]

    async def read_room(
        self, token: str, room_name: str, since: int | None = None, limit: int = DEFAULT_LIMIT
    ) -> tuple[list[Event], bool]:
        limit = _clamp_limit(limit)
        async with self._tx() as db:
            me = await self._auth(db, token)
            room = await self._member_room(db, me, room_name)
            if since is not None:
                return _page(
                    await db.fetch(
                        "SELECT * FROM events WHERE room = $1 AND recipient_id IS NULL "
                        "AND id > $2 ORDER BY id LIMIT $3",
                        room["name"], since, limit + 1,
                    ),
                    limit,
                )
            events, more = _page(
                await db.fetch(
                    "SELECT e.* FROM events e JOIN members m "
                    "ON m.room = e.room AND m.user_id = $2 "
                    "WHERE e.room = $1 AND e.recipient_id IS NULL AND e.id > m.last_read "
                    "AND e.sender_id <> $2 ORDER BY e.id LIMIT $3",
                    room["name"], me["id"], limit + 1,
                ),
                limit,
            )
            if events:
                await self._mark_room_read(db, me, room["name"], events[-1].id)
            return events, more

    async def read_private(
        self, token: str, since: int | None = None, limit: int = DEFAULT_LIMIT
    ) -> tuple[list[Event], bool]:
        limit = _clamp_limit(limit)
        async with self._tx() as db:
            me = await self._auth(db, token)
            if since is not None:
                return _page(
                    await db.fetch(
                        "SELECT * FROM events WHERE recipient_id IS NOT NULL "
                        "AND (recipient_id = $1 OR sender_id = $1) AND id > $2 "
                        "ORDER BY id LIMIT $3",
                        me["id"], since, limit + 1,
                    ),
                    limit,
                )
            events, more = _page(
                await db.fetch(
                    "SELECT * FROM events WHERE recipient_id = $1 AND id > $2 "
                    "ORDER BY id LIMIT $3",
                    me["id"], me["inbox_read"], limit + 1,
                ),
                limit,
            )
            if events:
                await self._mark_inbox_read(db, me, events[-1].id)
            return events, more

    async def read_all_unread(
        self, token: str, limit: int = DEFAULT_LIMIT
    ) -> tuple[list[Event], bool]:
        """Everything unread across joined rooms and private messages, oldest first."""
        limit = _clamp_limit(limit)
        async with self._tx() as db:
            me = await self._auth(db, token)
            events, more = _page(
                await db.fetch(
                    "SELECT e.* FROM events e "
                    "LEFT JOIN members m ON m.room = e.room AND m.user_id = $1 "
                    "WHERE e.sender_id <> $1 AND ("
                    "  (e.recipient_id = $1 AND e.id > $2) OR "
                    "  (e.recipient_id IS NULL AND m.user_id IS NOT NULL AND e.id > m.last_read)"
                    ") ORDER BY e.id LIMIT $3",
                    me["id"], me["inbox_read"], limit + 1,
                ),
                limit,
            )
            for event in events:
                if event.to:  # private, including kick notices
                    await self._mark_inbox_read(db, me, event.id)
                else:
                    await self._mark_room_read(db, me, event.room, event.id)
            return events, more

    async def wait_for_messages(
        self, token: str, timeout: float, limit: int = DEFAULT_LIMIT
    ) -> tuple[list[Event], bool]:
        """Block until something unread arrives, or the timeout passes."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, min(timeout, MAX_WAIT))
        while True:
            # Taken before reading, so a commit during the read still wakes us.
            changed = self._changed
            events, more = await self.read_all_unread(token, limit)
            remaining = deadline - loop.time()
            if events or remaining <= 0:
                return events, more
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except TimeoutError:
                pass

    # --- internals ----------------------------------------------------------

    def _now(self) -> datetime:
        return datetime.fromtimestamp(self._clock(), timezone.utc)

    async def _auth(self, db, token: str) -> asyncpg.Record:
        """Resolve a token to its user, and count this call as activity."""
        me = await db.fetchrow(
            "UPDATE users SET last_seen = $2 WHERE token_hash = $1 RETURNING *",
            _hash_token(token), self._now(),
        )
        if me is None:
            raise ChatError(
                "Unknown or expired token. You may have timed out, or the server may "
                "have restarted, which clears everything. Call sign_on to get a new one."
            )
        return me

    async def _add_event(
        self, db, kind: str, sender: asyncpg.Record, text: str = "",
        room: str | None = None, recipient: asyncpg.Record | None = None,
    ) -> Event:
        self._dirty = True
        return _event(
            await db.fetchrow(
                "INSERT INTO events (time, kind, sender_id, sender, text, room, recipient_id, recipient) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *",
                self._now(), kind, sender["id"], sender["name"], text, room,
                recipient["id"] if recipient else None,
                recipient["name"] if recipient else None,
            )
        )

    async def _find_user(self, db, screen_name: str) -> asyncpg.Record:
        user = await db.fetchrow(
            "SELECT * FROM users WHERE lower(name) = lower($1) AND signed_off IS NULL",
            screen_name,
        )
        if user is None:
            raise ChatError(f"{screen_name} is not signed on.")
        return user

    async def _find_room(self, db, room_name: str) -> asyncpg.Record:
        name = _room_name(room_name)
        room = await db.fetchrow("SELECT * FROM rooms WHERE name = $1", name)
        if room is None:
            raise ChatError(f"No such room: {name}.")
        return room

    async def _member_room(self, db, me: asyncpg.Record, room_name: str) -> asyncpg.Record:
        room = await self._find_room(db, room_name)
        member = await db.fetchval(
            "SELECT 1 FROM members WHERE room = $1 AND user_id = $2", room["name"], me["id"]
        )
        if not member:
            raise ChatError(f"You are not in {room['name']}. Call join_room first.")
        return room

    def _require_op(self, me: asyncpg.Record, room: asyncpg.Record) -> None:
        if room["op_id"] != me["id"]:
            raise ChatError(f"Only the operator of {room['name']} can do that.")

    async def _room_info(self, db, name: str) -> dict:
        room = await db.fetchrow(
            "SELECT r.topic, u.name AS op FROM rooms r LEFT JOIN users u ON u.id = r.op_id "
            "WHERE r.name = $1",
            name,
        )
        members = await db.fetch(
            "SELECT u.name FROM members m JOIN users u ON u.id = m.user_id "
            "WHERE m.room = $1 ORDER BY m.position",
            name,
        )
        history = await db.fetchval(
            "SELECT count(*) FROM events WHERE room = $1 AND recipient_id IS NULL", name
        )
        scrollback = await db.fetch(
            "SELECT * FROM (SELECT * FROM events WHERE room = $1 AND recipient_id IS NULL "
            "AND kind = 'message' ORDER BY id DESC LIMIT $2) recent ORDER BY id",
            name, SCROLLBACK,
        )
        return {
            "room": name,
            "topic": room["topic"],
            "op": room["op"],
            "members": [m["name"] for m in members],
            "history_available": history,
            "scrollback": [_event(row).to_dict() for row in scrollback],
        }

    async def _drop_member(self, db, room: str, user_id: int) -> None:
        await db.execute("DELETE FROM members WHERE room = $1 AND user_id = $2", room, user_id)
        # Longest-present member takes over as operator; an empty room is removed.
        await db.execute(
            "UPDATE rooms SET op_id = (SELECT user_id FROM members WHERE room = $1 "
            "ORDER BY position LIMIT 1) WHERE name = $1 AND op_id = $2",
            room, user_id,
        )
        await db.execute(
            "DELETE FROM rooms WHERE name = $1 AND NOT EXISTS "
            "(SELECT 1 FROM members WHERE room = $1)",
            room,
        )

    async def _remove_user(self, db, user: asyncpg.Record, message: str) -> None:
        rooms = await db.fetch("SELECT room FROM members WHERE user_id = $1", user["id"])
        for row in rooms:
            await self._add_event(db, "quit", user, message, room=row["room"])
            await self._drop_member(db, row["room"], user["id"])
        await db.execute(
            "UPDATE users SET signed_off = $2, token_hash = NULL WHERE id = $1",
            user["id"], self._now(),
        )
        self._dirty = True  # a waiter on this token needs to find out

    async def _sweep(self, db) -> None:
        cutoff = self._now() - timedelta(seconds=IDLE_TIMEOUT)
        idle = await db.fetch(
            "SELECT * FROM users WHERE signed_off IS NULL AND last_seen < $1", cutoff
        )
        for user in idle:
            await self._remove_user(db, user, "timed out")

    async def _check_rate(self, db, me: asyncpg.Record) -> None:
        now = self._now()
        count, oldest = await db.fetchrow(
            "SELECT count(*), min(time) FROM events "
            "WHERE sender_id = $1 AND kind = 'message' AND time > $2",
            me["id"], now - timedelta(seconds=RATE_WINDOW),
        )
        if count >= RATE_LIMIT:
            wait = int((oldest - now).total_seconds() + RATE_WINDOW) + 1
            raise ChatError(
                f"Rate limit: {RATE_LIMIT} messages per {int(RATE_WINDOW)}s. "
                f"Try again in {wait}s."
            )

    async def _mark_room_read(self, db, me: asyncpg.Record, room: str, event_id: int) -> None:
        await db.execute(
            "UPDATE members SET last_read = greatest(last_read, $3) "
            "WHERE room = $1 AND user_id = $2",
            room, me["id"], event_id,
        )

    async def _mark_inbox_read(self, db, me: asyncpg.Record, event_id: int) -> None:
        await db.execute(
            "UPDATE users SET inbox_read = greatest(inbox_read, $2) WHERE id = $1",
            me["id"], event_id,
        )

    def _describe(self, user: asyncpg.Record) -> dict:
        return {
            "screen_name": user["name"],
            "profile": user["profile"],
            "away": user["away"],
            "idle_seconds": int((self._now() - user["last_seen"]).total_seconds()),
        }
