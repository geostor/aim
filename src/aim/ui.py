"""The AIM console: a full-screen display, like the order screen in a restaurant kitchen.

You watch it from across the room. Each agent is a ticket showing what it last said
and how long it has been quiet, and the tickets change color as they go stale.

The rules that keep it fast and easy:
  - The screen shows the state; you never have to ask for it.
  - The layout never moves, and color is backed up by words.
  - Controls are one thin strip of single keys, which can also be clicked.
  - The UI thread never waits. Everything slow happens on background threads.

It uses only the Python standard library, and it does its work by running the `aim`
and `aimwatch` commands, so it holds no chat logic and no database code.
"""

from __future__ import annotations

import curses
import locale
import math
import os
import re
import shlex
import signal
import subprocess
import threading
import time
import traceback
import unicodedata
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

BIN = Path(__file__).resolve().parents[2] / "bin"
NAME = re.compile(r"[A-Za-z][A-Za-z0-9_-]{1,31}")
POLL_SECONDS = 1.5
MAX_EVENTS = 5000
MIN_HEIGHT, MIN_WIDTH = 16, 60
QUIET_SECONDS = 60  # a running ticket turns yellow after this long without a message
STUCK_SECONDS = 300  # and red after this long
TICKET_WIDTH = 34

# Color pairs.
TEXT, DIM, HEADER, GOOD, BAD, WARN, INFO, PRIVATE, FINAL = range(1, 10)
GREEN, RED, YELLOW, BLUE, CYAN, WHITE, OFF = range(10, 17)


# --- text ---------------------------------------------------------------------


def clean(text: str) -> str:
    """Remove anything that could move the cursor or reorder text on the screen."""
    text = text.replace("\t", " ")
    return "".join(c for c in text if unicodedata.category(c)[0] != "C")


def char_width(c: str) -> int:
    if unicodedata.combining(c) or unicodedata.category(c) == "Mn":
        return 0
    return 2 if unicodedata.east_asian_width(c) in "WF" else 1


def clip(text: str, width: int) -> str:
    used = 0
    for i, c in enumerate(text):
        used += char_width(c)
        if used > width:
            return text[:i]
    return text


def wrap(text: str, width: int) -> list[str]:
    """Break text into lines that fit, at spaces where possible."""
    width = max(1, width)
    lines, line, used, last_space = [], [], 0, -1
    for c in text:
        w = char_width(c)
        if used + w > width and line:
            if last_space > 0 and c != " ":
                head, tail = line[:last_space], line[last_space + 1 :]
            else:
                head, tail = line, []
            lines.append("".join(head))
            line, last_space = tail, -1
            used = sum(char_width(x) for x in line)
        if c == " ":
            last_space = len(line)
        line.append(c)
        used += w
    lines.append("".join(line))
    return lines


def seconds_since(clock: str) -> int:
    """Seconds since an HH:MM:SS time today, allowing for one midnight."""
    try:
        hours, minutes, seconds = (int(part) for part in clock.split(":"))
    except ValueError:
        return 0
    now = time.localtime()
    elapsed = (now.tm_hour - hours) * 3600 + (now.tm_min - minutes) * 60 + now.tm_sec - seconds
    return elapsed if elapsed >= 0 else elapsed + 86400


def short_age(seconds: int) -> str:
    if seconds < 3600:
        return f"{seconds // 60}:{seconds % 60:02d}"
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}"


# --- the background ----------------------------------------------------------


@dataclass
class Agent:
    name: str
    state: str
    time: str
    text: str


@dataclass
class Event:
    time: str
    kind: str
    sender: str
    target: str
    text: str


class Backend:
    """Runs commands and follows the conversation, without ever blocking the UI."""

    def __init__(self):
        self.lock = threading.Lock()
        self.server_on = False
        self.agents: list[Agent] = []
        self.events: deque[Event] = deque(maxlen=MAX_EVENTS)
        self.busy = ""
        self.notice = ""
        self.notice_ok = True
        self.version = 0
        self.jobs: deque[tuple] = deque()
        self.results: deque[Callable] = deque()
        self.wake_worker = threading.Event()
        self.wake_poller = threading.Event()
        self.feed_process: subprocess.Popen | None = None
        self.stopping = False
        self.config = self._read_config()
        self.log_path = Path(self.config.get("state", ".")) / "ui.log"

    def _read_config(self) -> dict[str, str]:
        result = subprocess.run(
            [str(BIN / "aim"), "config"], capture_output=True, text=True, timeout=30,
            stdin=subprocess.DEVNULL,
        )
        return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)

    def start(self) -> None:
        for target in (self._work, self._poll, self._follow):
            threading.Thread(target=self._guarded, args=(target,), daemon=True).start()

    def stop(self) -> None:
        self.stopping = True
        with self.lock:
            feed = self.feed_process
        if feed:
            feed.kill()

    def log(self, message: str) -> None:
        try:
            with self.log_path.open("a") as log:
                log.write(f"{time.strftime('%F %T')} {message}\n")
        except OSError:
            pass

    def _guarded(self, target: Callable) -> None:
        """Keep a background thread alive whatever goes wrong inside it."""
        while not self.stopping:
            try:
                target()
            except Exception:
                self.log(traceback.format_exc())
                time.sleep(1)

    def _touch(self) -> None:
        self.version += 1

    def say(self, message: str, ok: bool = True) -> None:
        with self.lock:
            self.notice, self.notice_ok = clean(message), ok
            self._touch()

    def run(self, label: str, args: list[str], then: Callable | None = None) -> None:
        """Queue a command. `then(stdout)` runs on the UI thread if it succeeds."""
        with self.lock:
            self.jobs.append((label, args, then))
        self.wake_worker.set()

    def _command(self, *args: str, timeout: float = 120) -> subprocess.CompletedProcess:
        return subprocess.run(
            [str(BIN / "aim"), *args], capture_output=True, text=True, errors="replace",
            stdin=subprocess.DEVNULL, start_new_session=True, timeout=timeout,
        )

    def _work(self) -> None:
        while not self.stopping:
            with self.lock:
                job = self.jobs.popleft() if self.jobs else None
                self.busy = job[0] if job else ""
                self._touch()
            if job is None:
                self.wake_worker.wait(1)
                self.wake_worker.clear()
                continue
            label, args, then = job
            try:
                result = self._command(*args)
                ok, output = result.returncode == 0, result.stdout + result.stderr
            except subprocess.TimeoutExpired:
                ok, output = False, f"{label} timed out"
            lines = [line for line in output.splitlines() if line.strip()]
            if then is None or not ok:
                # A multi-agent start reports each agent; the last line is the summary.
                last = lines[-1].removeprefix("aim: ") if lines else ""
                self.say(last or ("Done." if ok else f"{label} failed."), ok)
            if ok and then is not None:
                with self.lock:
                    self.results.append(lambda then=then, output=output: then(output))
                    self._touch()
            if ok and args[:1] == ["start"]:
                self.restart_feed()
            self.wake_poller.set()

    def _poll(self) -> None:
        while not self.stopping:
            try:
                status = self._command("status", timeout=15)
                listing = self._command("list", timeout=15)
            except subprocess.TimeoutExpired:
                status = listing = None
            if status is not None and listing is not None:
                agents = []
                for line in listing.stdout.splitlines():
                    fields = line.split("\t")
                    if len(fields) == 4:
                        agents.append(Agent(*[clean(f) for f in fields]))
                server_on = status.stdout.startswith("Server: running")
                with self.lock:
                    if (server_on, agents) != (self.server_on, self.agents):
                        self.server_on, self.agents = server_on, agents
                        self._touch()
            self.wake_poller.wait(POLL_SECONDS)
            self.wake_poller.clear()

    def restart_feed(self) -> None:
        with self.lock:
            feed = self.feed_process
        if feed:
            feed.kill()

    def _follow(self) -> None:
        """Stream the conversation from aimwatch, restarting it if it ever exits."""
        while not self.stopping:
            process = subprocess.Popen(
                [str(BIN / "aimwatch")], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, text=True, errors="replace", start_new_session=True,
                env=os.environ | {"AIM_WATCH_PARENT": str(os.getpid())},
            )
            with self.lock:
                self.feed_process = process
                self.events.clear()  # aimwatch replays everything when it starts
                self._touch()
            for line in process.stdout:
                fields = line.rstrip("\n").split("\t")
                if len(fields) >= 5:
                    event = Event(*[clean(f) for f in fields[:4]], clean(" ".join(fields[4:])))
                    with self.lock:
                        self.events.append(event)
                        self._touch()
                elif "wiped" in line:
                    with self.lock:
                        self.events.clear()
                        self._touch()
            process.wait()
            if not self.stopping:
                time.sleep(1)


# --- missions, which are plain files ----------------------------------------


class Missions:
    def __init__(self, folder: str):
        self.folder = Path(folder)

    def names(self) -> list[str]:
        try:
            return sorted(p.parent.name for p in self.folder.glob("*/mission.md"))
        except OSError:
            return []

    def prompt_path(self, name: str) -> Path:
        return self.folder / name / "mission.md"

    def team_path(self, name: str) -> Path:
        return self.folder / name / "team.tsv"

    def read(self, path: Path) -> list[str]:
        try:
            return [clean(line) for line in path.read_text(errors="replace").splitlines()]
        except OSError:
            return []

    def team(self, name: str) -> list[tuple[str, str]]:
        members = []
        for line in self.read_raw(self.team_path(name)):
            if line.strip() and not line.startswith("#"):
                member, _, rest = line.partition("\t")
                role, _, tools = rest.partition("\t")
                note = " (can write files)" if "Write" in tools else ""
                members.append((clean(member), clean(role) + note))
        return members

    def read_raw(self, path: Path) -> list[str]:
        try:
            return path.read_text(errors="replace").splitlines()
        except OSError:
            return []

    def create(self, name: str) -> None:
        folder = self.folder / name
        folder.mkdir(parents=True, exist_ok=True)
        self.prompt_path(name).write_text(
            "Replace this with what the agents should do, how they should work together,\n"
            "and how they know they are finished.\n"
        )
        self.team_path(name).write_text("# One agent per line: name, a tab, then its role.\n")


# --- screens -----------------------------------------------------------------


@dataclass
class Button:
    key: str
    label: str
    color: int
    action: Callable
    why_off: str = ""  # if set, the button is shown dimmed and says this when pressed

    @property
    def enabled(self) -> bool:
        return not self.why_off


class Screen:
    title = ""

    def __init__(self, app: App):
        self.app = app

    def buttons(self) -> list[Button]:
        return []

    def body(self, y: int, x: int, h: int, w: int) -> None:
        pass

    def on_key(self, key) -> bool:
        return False


@dataclass
class Ticket:
    name: str
    state: str = "ONLINE"
    messages: int = 0
    last_time: str = ""
    final: bool = False
    recent: deque | None = None


class Main(Screen):
    def __init__(self, app):
        super().__init__(app)
        self.scroll = 0
        self.full_conversation = False

    def buttons(self):
        app, b = self.app, self.app.backend
        server_off = "" if b.server_on else "Start the server first: press 1."
        no_agents = "" if b.agents else "There are no agents yet."
        if b.busy:
            toggle = Button("1", "WORKING...", OFF, lambda: None, "Please wait: " + b.busy)
        elif b.server_on:
            toggle = Button("1", "STOP SERVER", RED, app.ask_stop_server)
        else:
            toggle = Button("1", "START SERVER", GREEN, app.start_server)
        return [
            toggle,
            Button("2", "ADD AGENT", BLUE, app.add_agent, server_off),
            Button("3", "START MISSION", YELLOW, app.start_mission, server_off),
            Button("4", "REMOVE AGENT", RED, app.remove_agent, no_agents),
            Button("5", "VIEW AGENT", CYAN, app.view_agent, no_agents),
            Button("q", "QUIT", WHITE, app.quit),
        ]

    def on_key(self, key):
        if key == "\t":
            self.full_conversation = not self.full_conversation
            self.scroll = 0
            return True
        if key in ("v", "V"):
            self.app.view_page()
            return True
        page = max(1, self.app.body_height // 2)
        moves = {
            curses.KEY_UP: 1, curses.KEY_DOWN: -1,
            curses.KEY_PPAGE: page, curses.KEY_NPAGE: -page,
        }
        if key in moves:
            self.scroll = max(0, self.scroll + moves[key])
            return True
        if key == curses.KEY_END:
            self.scroll = 0
            return True
        if key == curses.KEY_HOME:
            self.scroll = 10**9
            return True
        return False

    def tickets(self) -> list[Ticket]:
        """One ticket per agent: ours in a stable order, then anyone else who showed up."""
        b = self.app.backend
        found: dict[str, Ticket] = {}
        for agent in b.agents:
            state = "RUNNING" if agent.state == "running" else "DONE"
            found[agent.name.lower()] = Ticket(agent.name, state, recent=deque(maxlen=20))
        ours = set(found)
        with b.lock:
            events = list(b.events)
        for event in events:
            ticket = found.setdefault(
                event.sender.lower(), Ticket(event.sender, recent=deque(maxlen=20))
            )
            if event.kind == "quit" and event.sender.lower() not in ours:
                ticket.state = "LEFT"
            elif event.kind == "join" and event.sender.lower() not in ours:
                ticket.state = "ONLINE"
            if event.kind == "message":
                ticket.messages += 1
                ticket.last_time = event.time
                private = not event.target.startswith("#")
                ticket.recent.append(f"> {event.target}: {event.text}" if private else event.text)
                if event.text.upper().lstrip("*# ").startswith("FINAL"):
                    ticket.final = True
        return list(found.values())

    def body(self, y, x, h, w):
        tickets = self.tickets()
        if self.full_conversation or not tickets:
            self.draw_feed(y, x, h, w)
            return
        rail = max(5, h // 3)
        self.draw_tickets(tickets, y, x, h - rail, w)
        self.draw_feed(y + h - rail, x, rail, w)

    def draw_tickets(self, tickets, y, x, h, w):
        columns = max(1, w // TICKET_WIDTH)
        rows = max(1, min(math.ceil(len(tickets) / columns), max(1, h // 5)))
        shown = tickets[: columns * rows]
        width, height = w // columns, h // rows
        for i, ticket in enumerate(shown):
            row, column = divmod(i, columns)
            self.draw_ticket(ticket, y + row * height, x + column * width, height, width - 1)
        hidden = len(tickets) - len(shown)
        if hidden:
            note = f" +{hidden} MORE: USE 5 "
            self.app.text(y + h - 1, x + w - len(note), note, WARN, len(note), curses.A_REVERSE)

    def draw_ticket(self, ticket, y, x, h, w):
        app = self.app
        quiet = seconds_since(ticket.last_time) if ticket.last_time else None
        status = ticket.state
        if ticket.final:
            color, status = YELLOW, "FINAL"
        elif ticket.state in ("DONE", "LEFT"):
            color = WHITE
        elif quiet is None:
            color = BLUE
        elif quiet >= STUCK_SECONDS:
            color, status = RED, "STUCK?"
        elif quiet >= QUIET_SECONDS:
            color, status = YELLOW, "QUIET"
        else:
            color = GREEN
        age = short_age(quiet) if quiet is not None else "--:--"
        right = f"{status} {age} "
        app.fill(y, x, 1, w, color)
        app.text(y, x + 1, ticket.name, color, max(0, w - len(right) - 2), curses.A_BOLD)
        app.text(y, x + w - len(right), right, color, len(right), curses.A_BOLD)
        app.hit(y, y + h, x, x + w, lambda name=ticket.name: app.open_agent(name))

        lines: list[str] = []
        room = h - 2
        for message in reversed(ticket.recent or ()):
            lines[:0] = wrap(message, w - 3) + [""]
            if len(lines) > room:
                break
        lines = lines[:-1] if lines and not lines[-1] else lines
        if not lines:
            app.text(y + 1, x + 1, "No messages yet.", DIM, w - 2)
        visible = lines[-room:] if room > 0 else []
        for i, line in enumerate(visible):
            newest = i >= len(visible) - len(wrap((ticket.recent or [""])[-1], w - 3))
            app.text(y + 1 + i, x + 1, line, TEXT if newest else DIM, w - 2)

    def draw_feed(self, y, x, h, w):
        app, b = self.app, self.app.backend
        label = "CONVERSATION (TAB: FULL SCREEN  V: VIEW PAGE)"
        if self.scroll:
            label += "  (SCROLLED: END KEY FOR LIVE)"
        app.panel_title(y, x, w, label)
        h -= 1
        y += 1
        with b.lock:
            events = list(b.events)
        if not events:
            if not b.server_on:
                hint = "Server is off. Press 1 to start it."
            elif not b.agents:
                hint = "Server is on. Press 3 to start a mission."
            else:
                hint = "Waiting for the agents to say something..."
            app.text(y + h // 2, x + max(0, (w - len(hint)) // 2), hint, WARN, w)
            return
        wanted = h + self.scroll
        lines: list[tuple[str, int, int]] = []
        for event in reversed(events):
            lines[:0] = self.format_event(event, w - 2)
            if len(lines) >= wanted:
                break
        self.scroll = min(self.scroll, max(0, len(lines) - h))
        end = len(lines) - self.scroll
        for i, (text, color, attr) in enumerate(lines[max(0, end - h) : end]):
            app.text(y + i, x + 1, text, color, w - 2, attr)

    def format_event(self, event: Event, width: int):
        stamp = event.time
        if event.kind == "message":
            private = not event.target.startswith("#")
            who = f"{event.sender} > {event.target}" if private else f"{event.sender} {event.target}"
            final = event.text.upper().lstrip("*# ").startswith("FINAL")
            color = PRIVATE if private else (FINAL if final else TEXT)
            attr = curses.A_BOLD if final else 0
            head = [(f"{stamp} {who}", INFO if not private else PRIVATE, curses.A_BOLD)]
            return head + [("  " + line, color, attr) for line in wrap(event.text, width - 2)]
        verbs = {
            "join": f"joined {event.target}", "part": f"left {event.target}",
            "quit": "signed off", "topic": f"set the topic: {event.text}",
            "kick": f"kicked {event.text}",
        }
        verb = verbs.get(event.kind, event.kind)
        return [(line, DIM, 0) for line in wrap(f"{stamp} {event.sender} {verb}", width)]


class Choose(Screen):
    """Pick from a list by number, arrows and Enter, or a click."""

    def __init__(self, app, title, items, on_pick, extra=None):
        super().__init__(app)
        self.title, self.items, self.on_pick = title, items, on_pick
        self.extra = extra or []
        self.selected = 0

    def buttons(self):
        return [
            *self.extra,
            Button("\n", "ENTER: SELECT", GREEN, self.pick, "" if self.items else "Nothing to pick."),
            Button("\x1b", "ESC: BACK", WHITE, self.app.pop),
        ]

    def pick(self, index=None):
        index = self.selected if index is None else index
        if 0 <= index < len(self.items):
            self.on_pick(self.items[index][0])

    def on_key(self, key):
        if isinstance(key, str) and key.isdigit() and key != "0":
            index = int(key) - 1
            if index < len(self.items):
                self.pick(index)
            else:
                self.app.backend.say(f"There is no number {key}.", False)
            return True
        if key == curses.KEY_UP:
            self.selected = max(0, self.selected - 1)
            return True
        if key == curses.KEY_DOWN:
            self.selected = min(len(self.items) - 1, self.selected + 1)
            return True
        return False

    def body(self, y, x, h, w):
        app = self.app
        app.panel_title(y, x, w, self.title)
        top = max(0, self.selected - (h - 3) // 2 + 1) // 2
        row = y + 2
        for i, (_, label, detail) in list(enumerate(self.items))[top:]:
            if row + 1 >= y + h:
                break
            attr = curses.A_REVERSE if i == self.selected else 0
            number = f"{i + 1:>2}" if i < 9 else "  "
            app.text(row, x + 2, f"{number}  {label}".ljust(w - 4), TEXT, w - 4, attr | curses.A_BOLD)
            if detail:
                app.text(row + 1, x + 6, detail, DIM, w - 8)
            app.hit(row, row + 2, x, x + w, lambda i=i: self.pick(i))
            row += 2


class TextEntry(Screen):
    def __init__(self, app, title, hint, on_done, validate=None):
        super().__init__(app)
        self.title, self.hint, self.on_done = title, hint, on_done
        self.validate = validate or (lambda value: "" if value.strip() else "Type something first.")
        self.value = ""

    def submit(self):
        problem = self.validate(self.value)
        if problem:
            self.app.backend.say(problem, False)
        else:
            self.on_done(self.value.strip())

    def buttons(self):
        return [
            Button("\n", "ENTER: OK", GREEN, self.submit),
            Button("\x1b", "ESC: BACK", WHITE, self.app.pop),
        ]

    def on_key(self, key):
        if key in ("\x7f", "\b", curses.KEY_BACKSPACE):
            self.value = self.value[:-1]
            return True
        if isinstance(key, str) and key not in ("\n", "\r", "\x1b") and clean(key):
            self.value += clean(key)
            return True
        return False

    def body(self, y, x, h, w):
        app = self.app
        app.panel_title(y, x, w, self.title)
        app.text(y + 2, x + 2, self.hint, DIM, w - 4)
        shown = self.value[-(w - 8) :]
        app.text(y + 4, x + 2, "> " + shown + "_", TEXT, w - 4, curses.A_BOLD)


class Document(Screen):
    """Scrolling text with buttons, for reviewing a mission or reading an agent."""

    def __init__(self, app, title, lines, button_maker):
        super().__init__(app)
        self.title, self.lines, self.button_maker = title, lines, button_maker
        self.scroll = 0

    def buttons(self):
        return self.button_maker()

    def on_key(self, key):
        page = max(1, self.app.body_height - 2)
        moves = {
            curses.KEY_UP: -1, curses.KEY_DOWN: 1,
            curses.KEY_PPAGE: -page, curses.KEY_NPAGE: page,
        }
        if key in moves:
            self.scroll = max(0, self.scroll + moves[key])
            return True
        return False

    def body(self, y, x, h, w):
        app = self.app
        app.panel_title(y, x, w, self.title)
        visual = []
        for text, color, attr in self.lines:
            visual += [(line, color, attr) for line in wrap(text, w - 4)]
        room = h - 2
        self.scroll = min(self.scroll, max(0, len(visual) - room))
        for i, (text, color, attr) in enumerate(visual[self.scroll : self.scroll + room]):
            app.text(y + 1 + i, x + 2, text, color, w - 4, attr)
        if len(visual) > room:
            shown = min(len(visual), self.scroll + room)
            app.text(y + h - 1, x + 2, f"UP/DOWN TO SCROLL ({shown}/{len(visual)})", WARN, w - 4)


# --- the app -----------------------------------------------------------------


class App:
    def __init__(self, stdscr, backend: Backend):
        self.screen = stdscr
        self.backend = backend
        self.missions = Missions(backend.config.get("missions", "missions"))
        self.budget = backend.config.get("budget", "?")
        self.stack: list[Screen] = [Main(self)]
        self.hits: list[tuple] = []
        self.body_height = 10
        self.running = True

    # --- navigation

    def push(self, screen: Screen) -> None:
        self.stack.append(screen)

    def pop(self) -> None:
        if len(self.stack) > 1:
            self.stack.pop()

    def home(self) -> None:
        del self.stack[1:]

    def quit(self) -> None:
        self.running = False

    def confirm(self, title, lines, yes_label, on_yes, color=RED):
        def choose_yes():
            self.pop()
            on_yes()

        self.push(Document(self, title, [(line, TEXT, 0) for line in lines], lambda: [
            Button("y", f"Y: {yes_label}", color, choose_yes),
            Button("n", "N: GO BACK", WHITE, self.pop),
            Button("\x1b", "ESC: GO BACK", WHITE, self.pop),
        ]))

    # --- actions

    def start_server(self):
        self.backend.run("Starting the server", ["start"])

    def ask_stop_server(self):
        running = sum(a.state == "running" for a in self.backend.agents)
        lines = ["This stops the server."]
        if running:
            lines.append(f"It also stops the {running} agent(s) still running.")
        lines.append("The conversation is kept until the next start, then saved to runs/.")
        self.confirm("STOP THE SERVER?", lines, "STOP SERVER",
                     lambda: self.backend.run("Stopping the server", ["stop"]))

    def choose_mission(self, on_pick):
        def new_mission():
            self.push(TextEntry(self, "NEW MISSION", "Name it with letters, digits, - or _.",
                                self.create_mission(on_pick), self.check_new_mission))

        items = [(name, name, self.mission_summary(name)) for name in self.missions.names()]
        no_missions = "" if items else "There are no missions to remove."
        self.push(Choose(self, "PICK A MISSION", items, on_pick, [
            Button("n", "N: NEW MISSION", BLUE, new_mission),
            Button("r", "R: REMOVE A MISSION", RED, self.remove_mission, no_missions),
        ]))

    def view_page(self):
        def picked(name):
            self.home()
            self.backend.run(f"Opening {name}", ["view", name])

        items = [(name, name, self.mission_summary(name)) for name in self.missions.names()]
        self.push(Choose(self, "VIEW WHICH MISSION'S PAGE?", items, picked))

    def remove_mission(self):
        def picked(name):
            def remove():
                self.home()
                self.backend.run(f"Removing {name}", ["rm-mission", name])

            self.pop()
            self.confirm(
                f"REMOVE MISSION {name}?",
                [f"This takes {name} off the list, including its prompt and team.",
                 "It's moved to .aim/removed-missions, so you can restore it from there.",
                 "Agents already running keep working."],
                "REMOVE MISSION", remove,
            )

        items = [(name, name, self.mission_summary(name)) for name in self.missions.names()]
        self.push(Choose(self, "REMOVE WHICH MISSION?", items, picked))

    def mission_summary(self, name):
        lines = self.missions.read(self.missions.prompt_path(name))
        count = len(self.missions.team(name))
        first = next((line for line in lines if line.strip()), "")
        return f"{count} agent(s). {first}"

    def check_new_mission(self, value):
        value = value.strip()
        if not NAME.fullmatch(value):
            return "Use 2-32 letters, digits, - or _, starting with a letter."
        if value in self.missions.names():
            return "That mission already exists."
        return ""

    def create_mission(self, on_pick):
        def create(name):
            try:
                self.missions.create(name)
            except OSError as e:
                self.backend.say(f"Couldn't create the mission: {e}", False)
                return
            self.pop()
            self.edit(self.missions.prompt_path(name))
            on_pick(name)

        return create

    def start_mission(self):
        self.choose_mission(self.review_team)

    def review_team(self, mission):
        def lines():
            team = self.missions.team(mission)
            out = []
            if team:  # first, so a long prompt can't push the cost off the screen
                out += [(f"Together they can spend at most {self.spend(len(team))}.", WARN, curses.A_BOLD), ("", TEXT, 0)]
            out += [(line, TEXT, 0) for line in self.missions.read(self.missions.prompt_path(mission))]
            out.append(("", TEXT, 0))
            out.append(("TEAM", WARN, curses.A_BOLD))
            for member, role in team:
                out.append((f"{member}: {role}", INFO, 0))
            out += self.protocol_lines(mission)
            if not team:
                out.insert(0, ("No agents yet. Press T to add them.", BAD, curses.A_BOLD))
            return out

        def buttons():
            team = self.missions.team(mission)
            doc.lines = lines()
            return [
                Button("s", f"S: START {len(team)} AGENTS", GREEN, start,
                       "" if team else "Add agents first: press T."),
                Button("e", "E: EDIT PROMPT", BLUE,
                       lambda: self.edit(self.missions.prompt_path(mission))),
                Button("t", "T: EDIT TEAM", BLUE,
                       lambda: self.edit(self.missions.team_path(mission))),
                Button("\x1b", "ESC: CANCEL", WHITE, self.pop),
            ]

        def start():
            self.home()
            self.backend.run(f"Starting {mission}", ["team", mission])

        doc = Document(self, f"MISSION: {mission}", lines(), buttons)
        self.push(doc)

    def protocol_lines(self, mission: str):
        """The team rules every agent also gets, so the review shows the whole prompt."""
        try:
            result = subprocess.run(
                [str(BIN / "aim"), "protocol", mission], capture_output=True, text=True,
                errors="replace", stdin=subprocess.DEVNULL, timeout=5,
            )
            text = result.stdout
        except (OSError, subprocess.SubprocessError):
            text = ""
        heading = ("TEAM RULES (protocol.md)", WARN, curses.A_BOLD)
        return [("", TEXT, 0), heading, *[(clean(line), TEXT, 0) for line in text.splitlines()]]

    def spend(self, agents: int) -> str:
        try:
            return f"${float(self.budget) * agents:g}"
        except ValueError:
            return "an unknown amount"

    def add_agent(self):
        def picked(mission):
            self.push(TextEntry(self, "AGENT NAME", "Letters, digits, - or _.",
                                lambda name: named(mission, name), self.check_agent_name))

        def named(mission, name):
            self.push(TextEntry(self, f"ROLE FOR {name}",
                                "In a sentence: what should this agent do?",
                                lambda role: review(mission, name, role)))

        def review(mission, name, role):
            def lines():
                out = [(f"This agent can spend at most {self.spend(1)}.", WARN, curses.A_BOLD), ("", TEXT, 0)]
                out += [(line, TEXT, 0) for line in self.missions.read(self.missions.prompt_path(mission))]
                out += [("", TEXT, 0), (f"You are {name}. {role}", INFO, curses.A_BOLD)]
                out += self.protocol_lines(mission)
                return out

            def buttons():
                doc.lines = lines()
                return [
                    Button("s", f"S: START {name}", GREEN, start),
                    Button("e", "E: EDIT PROMPT", BLUE,
                           lambda: self.edit(self.missions.prompt_path(mission))),
                    Button("\x1b", "ESC: CANCEL", WHITE, self.pop),
                ]

            def start():
                self.home()
                self.backend.run(f"Starting {name}", ["add", name, role, mission])

            doc = Document(self, f"START {name} ON {mission}", lines(), buttons)
            self.push(doc)

        self.choose_mission(picked)

    def check_agent_name(self, value):
        value = value.strip()
        if not NAME.fullmatch(value):
            return "Use 2-32 letters, digits, - or _, starting with a letter."
        if any(a.name == value and a.state == "running" for a in self.backend.agents):
            return f"{value} is already running."
        return ""

    def agent_items(self):
        items = []
        for agent in self.backend.agents:
            detail = "(no messages yet)" if agent.text == "-" else agent.text
            items.append((agent.name, f"{agent.name}  {agent.state.upper()}", detail))
        return items

    def remove_agent(self):
        def picked(name):
            self.pop()
            self.confirm(f"REMOVE {name}?", [f"This stops {name} and signs it off."],
                         "REMOVE", lambda: self.backend.run(f"Removing {name}", ["rm", name]))

        self.push(Choose(self, "REMOVE WHICH AGENT?", self.agent_items(), picked))

    def view_agent(self):
        def picked(name):
            self.pop()
            self.open_agent(name)

        self.push(Choose(self, "VIEW WHICH AGENT?", self.agent_items(), picked))

    def open_agent(self, name):
        doc = Document(self, f"AGENT: {name}", [("Loading...", DIM, 0)], lambda: [
            Button("r", "R: REFRESH", BLUE, load),
            Button("\x1b", "ESC: BACK", WHITE, self.pop),
        ])

        def show(output):
            doc.lines = [
                (clean(line), WARN if line.startswith("---") else TEXT,
                 curses.A_BOLD if line.startswith("---") else 0)
                for line in output.splitlines()
            ]

        def load():
            self.backend.run(f"Loading {name}", ["show", name], show)

        self.push(doc)
        load()

    def edit(self, path: Path) -> None:
        """Hand the terminal to the user's editor, then take it back."""
        editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "nano"
        curses.def_prog_mode()
        curses.endwin()
        try:
            subprocess.run([*shlex.split(editor), str(path)])
        except (OSError, ValueError, subprocess.SubprocessError) as e:
            self.backend.say(f"Couldn't open the editor: {e}", False)
        curses.reset_prog_mode()
        self.screen.clear()
        self.screen.refresh()

    # --- drawing

    def text(self, y, x, text, color=TEXT, width=None, attr=0):
        """Draw text that can never run off the screen or raise."""
        height, screen_width = self.screen.getmaxyx()
        if y < 0 or y >= height or x < 0 or x >= screen_width:
            return
        room = screen_width - x
        if width is not None:
            room = min(room, width)
        if room <= 0:
            return
        try:
            self.screen.addstr(y, x, clip(text, room), curses.color_pair(color) | attr)
        except curses.error:
            pass  # writing the bottom-right cell always reports an error

    def fill(self, y, x, h, w, color):
        for row in range(h):
            self.text(y + row, x, " " * w, color, w)

    def vline(self, y, x, h):
        for row in range(h):
            self.text(y + row, x, "|", DIM, 1)

    def panel_title(self, y, x, w, title):
        self.text(y, x, f" {title} ".ljust(max(0, w)), HEADER, w, curses.A_BOLD)

    def hit(self, y0, y1, x0, x1, action):
        self.hits.append((y0, y1, x0, x1, action))

    def draw(self):
        scr, b = self.screen, self.backend
        scr.erase()
        self.hits = []
        h, w = scr.getmaxyx()
        if h < MIN_HEIGHT or w < MIN_WIDTH:
            self.text(0, 0, f"MAKE THIS WINDOW BIGGER (AT LEAST {MIN_WIDTH}x{MIN_HEIGHT})", WARN)
            scr.noutrefresh()
            curses.doupdate()
            return
        self.draw_header(w)
        self.draw_notice(1, w)
        screen = self.stack[-1]
        buttons = screen.buttons()
        grid_top = h - 1
        self.body_height = grid_top - 2
        screen.body(2, 0, self.body_height, w)
        self.draw_buttons(buttons, grid_top, w)
        scr.noutrefresh()
        curses.doupdate()

    def draw_header(self, w):
        b = self.backend
        running = sum(a.state == "running" for a in b.agents)
        with b.lock:
            messages = sum(e.kind == "message" for e in b.events)
        self.text(0, 0, " " * w, HEADER, w)
        self.text(0, 1, "AIM", HEADER, 4, curses.A_BOLD)
        left = "AGENTIC INSTANT MESSENGER"
        if w >= 90:
            self.text(0, 6, left, HEADER, len(left))
        state, color = ("SERVER ON", GREEN) if b.server_on else ("SERVER OFF", RED)
        stats = f" AGENTS {running}/{len(b.agents)}  MSGS {messages}  {time.strftime('%H:%M:%S')} "
        x = w - len(stats)
        self.text(0, x, stats, HEADER, len(stats))
        self.text(0, x - len(state) - 3, f" {state} ", color, len(state) + 2, curses.A_BOLD)

    def draw_notice(self, y, w):
        b = self.backend
        if b.busy:
            self.text(y, 1, f"WORKING: {b.busy}...", WARN, w - 2, curses.A_BOLD)
        elif b.notice:
            self.text(y, 1, b.notice, GOOD if b.notice_ok else BAD, w - 2, curses.A_BOLD)

    def draw_buttons(self, buttons, y, w):
        """One thin strip of controls. Each keeps the same slot whatever the state."""
        for i, button in enumerate(buttons):
            x0 = i * w // len(buttons)
            x1 = (i + 1) * w // len(buttons)
            color = button.color if button.enabled else OFF
            self.fill(y, x0, 1, x1 - x0 - 1, color)
            label = clip(self.key_label(button), x1 - x0 - 2)
            self.text(y, x0 + 1, label, color, x1 - x0 - 2, curses.A_BOLD)
            self.hit(y, y + 1, x0, x1, lambda b=button: self.press(b))

    def key_label(self, button):
        if button.key.isdigit():
            return f"{button.key}  {button.label}"
        if button.key == "q":
            return f"Q  {button.label}"
        return button.label

    # --- input

    def press(self, button: Button) -> None:
        if button.enabled:
            button.action()
        else:
            self.backend.say(button.why_off, False)

    def handle(self, key) -> None:
        if key == "\r":
            key = "\n"
        if isinstance(key, int) and curses.KEY_F1 <= key <= curses.KEY_F1 + 5:
            key = str(key - curses.KEY_F1 + 1)
        if key == curses.KEY_ENTER:
            key = "\n"
        if key == curses.KEY_RESIZE:
            return
        if key == curses.KEY_MOUSE:
            self.handle_mouse()
            return
        screen = self.stack[-1]
        if screen.on_key(key):
            return
        if isinstance(key, str):
            for button in screen.buttons():
                if button.key == key.lower():
                    self.press(button)
                    return

    def handle_mouse(self) -> None:
        try:
            _, x, y, _, state = curses.getmouse()
        except curses.error:
            return
        screen = self.stack[-1]
        if state & curses.BUTTON4_PRESSED:
            screen.on_key(curses.KEY_UP)
        elif state & getattr(curses, "BUTTON5_PRESSED", 0):
            screen.on_key(curses.KEY_DOWN)
        elif state & (curses.BUTTON1_PRESSED | curses.BUTTON1_CLICKED):
            for y0, y1, x0, x1, action in reversed(self.hits):
                if y0 <= y < y1 and x0 <= x < x1:
                    action()
                    return

    def run(self) -> None:
        self.screen.timeout(100)
        self.screen.keypad(True)
        drawn = (None, None, None)
        while self.running:
            b = self.backend
            with b.lock:
                results = list(b.results)
                b.results.clear()
            for result in results:
                self.guard(result)
            state = (b.version, time.strftime("%H:%M:%S"), len(self.stack))
            if state != drawn:
                self.guard(self.draw)
                drawn = state
            try:
                key = self.screen.get_wch()
            except curses.error:
                continue  # no key within the timeout
            self.guard(lambda k=key: self.handle(k))
            self.guard(self.draw)

    def guard(self, action: Callable) -> None:
        """A bug in one action shouldn't take the whole console down."""
        try:
            action()
        except Exception:
            self.backend.log(traceback.format_exc())
            self.backend.say(f"Internal error, logged to {self.backend.log_path}", False)


def setup_colors() -> None:
    curses.start_color()
    pairs = {
        TEXT: (curses.COLOR_WHITE, curses.COLOR_BLACK),
        DIM: (curses.COLOR_CYAN, curses.COLOR_BLACK),
        HEADER: (curses.COLOR_WHITE, curses.COLOR_BLUE),
        GOOD: (curses.COLOR_GREEN, curses.COLOR_BLACK),
        BAD: (curses.COLOR_RED, curses.COLOR_BLACK),
        WARN: (curses.COLOR_YELLOW, curses.COLOR_BLACK),
        INFO: (curses.COLOR_CYAN, curses.COLOR_BLACK),
        PRIVATE: (curses.COLOR_MAGENTA, curses.COLOR_BLACK),
        FINAL: (curses.COLOR_YELLOW, curses.COLOR_BLACK),
        GREEN: (curses.COLOR_BLACK, curses.COLOR_GREEN),
        RED: (curses.COLOR_WHITE, curses.COLOR_RED),
        YELLOW: (curses.COLOR_BLACK, curses.COLOR_YELLOW),
        BLUE: (curses.COLOR_WHITE, curses.COLOR_BLUE),
        CYAN: (curses.COLOR_BLACK, curses.COLOR_CYAN),
        WHITE: (curses.COLOR_BLACK, curses.COLOR_WHITE),
        OFF: (curses.COLOR_CYAN, curses.COLOR_BLACK),
    }
    for pair, (fg, bg) in pairs.items():
        curses.init_pair(pair, fg, bg)


def console(stdscr, backend: Backend) -> None:
    curses.curs_set(0)
    setup_colors()
    stdscr.bkgd(" ", curses.color_pair(TEXT))
    curses.mousemask(curses.ALL_MOUSE_EVENTS)
    curses.mouseinterval(0)
    App(stdscr, backend).run()


def main() -> None:
    locale.setlocale(locale.LC_ALL, "")
    os.environ.setdefault("ESCDELAY", "25")  # make the Esc key feel instant

    def leave(*_):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, leave)
    signal.signal(signal.SIGHUP, leave)

    backend = Backend()
    backend.start()
    try:
        curses.wrapper(console, backend)
    except KeyboardInterrupt:
        pass
    finally:
        backend.stop()


if __name__ == "__main__":
    main()
