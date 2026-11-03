"""The console, driven through a real pseudo-terminal, read through a terminal emulator."""

import os
import socket
import stat
import subprocess
import time
from pathlib import Path

import pexpect
import pyte
import pytest

BIN = Path(__file__).parent.parent / "bin"
PORT = 18792


class Console:
    def __init__(self, env, rows=30, cols=110):
        self.screen = pyte.Screen(cols, rows)
        self.stream = pyte.Stream(self.screen)
        self.child = pexpect.spawn(
            str(BIN / "aim"), env=env | {"TERM": "xterm-256color"}, encoding="utf-8",
            dimensions=(rows, cols), timeout=1,
        )

    def pump(self):
        try:
            while True:
                self.stream.feed(self.child.read_nonblocking(65536, timeout=0.05))
        except (pexpect.TIMEOUT, pexpect.EOF):
            pass

    def text(self):
        self.pump()
        return "\n".join(self.screen.display)

    def wait(self, wanted, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if wanted in self.text():
                return
            time.sleep(0.05)
        raise AssertionError(f"never saw {wanted!r} on:\n{self.text()}")

    def wait_gone(self, wanted, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if wanted not in self.text():
                return
            time.sleep(0.05)
        raise AssertionError(f"{wanted!r} never went away:\n{self.text()}")

    def press(self, keys):
        self.child.send(keys)

    def click(self, wanted):
        """Click on some text, encoded as terminals report it: press, then release."""
        self.pump()
        for y, line in enumerate(self.screen.display):
            x = line.find(wanted)
            if x >= 0:
                position = bytes([33 + x + 1, 33 + y])
                os.write(self.child.child_fd, b"\x1b[M " + position + b"\x1b[M#" + position)
                return
        raise AssertionError(f"nothing to click: {wanted!r}")

    def alive(self):
        self.pump()
        return self.child.isalive()


@pytest.fixture
def env(tmp_path, test_database):
    fake = tmp_path / "claude"
    fake.write_text('#!/bin/sh\ntrap "exit 0" TERM\nsleep 60 & wait\n')
    editor = tmp_path / "editor"
    editor.write_text('#!/bin/sh\necho "edited line" >> "$1"\n')
    for f in (fake, editor):
        f.chmod(f.stat().st_mode | stat.S_IEXEC)
    missions = tmp_path / "missions"
    (missions / "plan").mkdir(parents=True)
    (missions / "plan" / "mission.md").write_text("Original prompt.\n")
    (missions / "plan" / "team.tsv").write_text("ann\tYou are friendly.\nben\tYou are terse.\n")
    environ = os.environ | {
        "AIM_RUNS": str(tmp_path / "runs"), "AIM_DB": "aim_test", "AIM_PORT": str(PORT),
        "AIM_CLAUDE": str(fake), "AIM_STATE": str(tmp_path / "state"),
        "AIM_MISSIONS": str(missions), "VISUAL": str(editor),
    }
    yield environ
    subprocess.run([BIN / "aim", "stop"], env=environ, capture_output=True, timeout=30)


def aim(env, *args):
    return subprocess.run([BIN / "aim", *args], env=env, capture_output=True, text=True, timeout=60)


def say(sender, text, target="#project", recipient=None):
    subprocess.run(
        ["podman", "exec", "aim-postgres", "psql", "-U", "aim", "-d", "aim_test", "-c",
         "INSERT INTO aim.users (name, signed_on, last_seen, signed_off) "
         f"VALUES ('{sender}', now(), now(), now());"
         "INSERT INTO aim.events (time, kind, sender_id, sender, text, room, recipient_id, recipient) "
         f"SELECT now(), 'message', max(id), '{sender}', {text}, "
         + ("NULL, max(id), '" + recipient + "'" if recipient else f"'{target}', NULL, NULL")
         + " FROM aim.users"],
        check=True, capture_output=True,
    )


@pytest.fixture
def ui(env):
    console = Console(env)
    console.wait("START SERVER")
    yield console
    console.child.terminate(force=True)


def test_first_screen_says_what_to_do_and_offers_only_what_works(ui):
    ui.wait("SERVER OFF")
    ui.wait("Press 1 to start it")
    for label in ("ADD AGENT", "START MISSION", "REMOVE AGENT", "VIEW AGENT", "QUIT"):
        assert label in ui.text()
    ui.press("3")
    ui.wait("Start the server first")
    assert "PICK A MISSION" not in ui.text()


def test_whole_shift_start_server_run_mission_read_remove_stop(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    ui.wait("Press 3 to start a mission")

    ui.press("3")
    ui.wait("PICK A MISSION")
    ui.wait("2 agent(s). Original prompt.")
    ui.press("1")  # one key picks it
    ui.wait("MISSION: plan")
    ui.wait("Original prompt.")
    ui.wait("ann: You are friendly.")
    ui.wait("TEAM RULES")  # the review shows the whole prompt
    ui.wait("Join #plan")
    ui.wait("START 2 AGENTS")
    ui.wait("can spend at most $4")
    assert "ann" not in aim(env, "list").stdout  # nothing starts before S

    ui.press("e")
    ui.wait("edited line")
    ui.press("s")
    ui.wait("RUNNING")
    ui.wait("AGENTS 2/2")

    say("ann", "'hello from ann'")
    ui.wait("hello from ann")
    ui.wait("MSGS 1")

    ui.press("5")
    ui.wait("VIEW WHICH AGENT?")
    ui.press("1")
    ui.wait("What ann has said")
    ui.wait("still running")
    ui.press("\x1b")
    ui.wait("CONVERSATION")

    ui.press("4")
    ui.wait("REMOVE WHICH AGENT?")
    ui.press("2")
    ui.wait("REMOVE ben?")
    ui.press("n")  # going back keeps it
    ui.wait("CONVERSATION")
    assert "ben" in aim(env, "list").stdout
    ui.press("4")
    ui.wait("REMOVE WHICH AGENT?")
    ui.press("2")
    ui.wait("REMOVE ben?")
    ui.press("y")
    ui.wait("AGENTS 1/1")

    ui.press("1")
    ui.wait("STOP THE SERVER?")
    ui.wait("1 agent(s) still running")
    ui.press("y")
    ui.wait("SERVER OFF")


def test_mouse_clicks_work_like_keys(ui):
    ui.click("START SERVER")
    ui.wait("SERVER ON")
    ui.click("START MISSION")
    ui.wait("PICK A MISSION")
    ui.wait("ESC: BACK")
    ui.click("ESC: BACK")
    ui.wait("CONVERSATION")


def test_clicks_work_on_the_far_side_of_a_wide_window(env):
    console = Console(env, rows=30, cols=160)
    console.wait("QUIT")
    console.pump()
    assert max(line.find("QUIT") for line in console.screen.display) > 94
    console.click("QUIT")
    console.child.expect(pexpect.EOF, timeout=10)


def test_new_mission_is_named_then_edited_and_needs_a_team(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    ui.press("3")
    ui.wait("PICK A MISSION")
    ui.press("n")
    ui.wait("NEW MISSION")
    ui.press("../bad\r")
    ui.wait("Use 2-32 letters")
    ui.press("\x7f" * 6 + "fresh\r")
    ui.wait("MISSION: fresh")
    ui.wait("edited line")
    ui.wait("No agents yet")
    ui.press("s")
    ui.wait("Add agents first")
    assert "fresh" in os.listdir(env["AIM_MISSIONS"])
    assert not (Path(env["AIM_MISSIONS"]).parent / "bad").exists()


def test_removing_a_mission_asks_first_and_keeps_a_copy(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    ui.press("3")
    ui.wait("PICK A MISSION")
    ui.press("r")
    ui.wait("REMOVE WHICH MISSION?")
    ui.press("1")
    ui.wait("REMOVE MISSION plan?")
    ui.press("n")  # going back keeps it
    ui.wait("PICK A MISSION")
    assert (Path(env["AIM_MISSIONS"]) / "plan" / "mission.md").exists()

    ui.press("r")
    ui.wait("REMOVE WHICH MISSION?")
    ui.press("1")
    ui.wait("REMOVE MISSION plan?")
    ui.press("y")
    ui.wait("Removed mission plan")
    assert not (Path(env["AIM_MISSIONS"]) / "plan").exists()
    kept = list((Path(env["AIM_STATE"]) / "removed-missions").glob("plan-*/mission.md"))
    assert len(kept) == 1 and "Original prompt." in kept[0].read_text()


def test_rm_mission_only_touches_real_missions(env):
    outside = Path(env["AIM_MISSIONS"]).parent / "outside"
    outside.mkdir()
    (outside / "mission.md").write_text("keep\n")
    for name in ("../outside", "nope", ""):
        assert aim(env, "rm-mission", name).returncode != 0
    assert (outside / "mission.md").exists()
    assert aim(env, "rm-mission", "plan").returncode == 0
    assert aim(env, "rm-mission", "plan").returncode != 0


def test_add_a_single_agent(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    ui.press("2")
    ui.wait("PICK A MISSION")
    ui.press("1")
    ui.wait("AGENT NAME")
    ui.press("zed\r")
    ui.wait("ROLE FOR zed")
    ui.press("Be quiet.\r")
    ui.wait("You are zed. Be quiet.")
    ui.press("s")
    ui.wait("zed")
    ui.wait("AGENTS 1/1")
    assert "zed\trunning" in aim(env, "list").stdout


def test_hostile_and_awkward_text_cant_break_the_screen(ui):
    ui.press("1")
    ui.wait("SERVER ON")
    say("ann", r"E'\033[2J\033[31mCLEARED \u202Ereversed\u202C \U0001F44B \u6C49\u5B57 end-of-text'")
    say("ben", "'" + "long " * 2000 + "END'")
    say("cat", "'psst'", recipient="ann")
    ui.wait("cat > ann")
    assert "STOP SERVER" in ui.text() and "SERVER ON" in ui.text()
    ui.press("\t")  # the full conversation
    ui.wait("END")
    ui.press("\x1bOH")  # Home: back to the first message
    ui.wait("CLEARED")
    ui.wait("SCROLLED")
    assert "STOP SERVER" in ui.text() and "SERVER ON" in ui.text()
    ui.press("\x1bOF")  # End: live again
    ui.wait_gone("SCROLLED")
    assert ui.alive()


def test_each_agent_gets_a_ticket_with_its_latest_message(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    aim(env, "team", "plan")
    ui.wait("AGENTS 2/2")
    ui.wait("No messages yet.")
    say("ann", "'ann is on it'")
    ui.wait("RUNNING 0:0")
    ui.wait("ann is on it")
    say("visitor", "'I connected some other way'")
    ui.wait("visitor")  # anyone who talks gets a ticket


def test_tickets_warn_when_an_agent_goes_quiet_and_flag_final(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    aim(env, "add", "ann", "friendly", "plan")
    aim(env, "add", "ben", "terse", "plan")
    aim(env, "add", "cat", "calm", "plan")
    def backdated(sender, text, seconds):
        say(sender, text)
        subprocess.run(
            ["podman", "exec", "aim-postgres", "psql", "-U", "aim", "-d", "aim_test", "-c",
             f"UPDATE aim.events SET time = now() - interval '{seconds} seconds' "
             f"WHERE sender = '{sender}'"],
            check=True, capture_output=True,
        )
    backdated("ann", "'been a while'", 120)
    backdated("ben", "'been ages'", 600)
    aim(env, "stop")
    say_after = "'FINAL: the answer'"
    ui.wait("SERVER OFF")
    ui.press("q")
    ui.child.expect(pexpect.EOF, timeout=10)
    again = Console(env)
    again.wait("QUIET 2:0")
    again.wait("STUCK? 10:0")
    say("cat", say_after)
    again.wait("FINAL 0:0")
    again.child.terminate(force=True)


def test_killing_the_console_outright_leaves_nothing_behind(env):
    console = Console(env)
    console.wait("START SERVER")
    token = env["AIM_STATE"]
    watcher = subprocess.run(["pgrep", "-f", "bin/aimwatch"], capture_output=True, text=True)
    before = set(watcher.stdout.split())
    console.child.kill(9)
    deadline = time.time() + 10
    while time.time() < deadline:
        remaining = subprocess.run(["pgrep", "-f", "bin/aimwatch"], capture_output=True, text=True)
        if len(set(remaining.stdout.split())) < len(before):
            return
        time.sleep(0.2)
    raise AssertionError(f"aimwatch outlived the console ({token})")


def test_resizing_and_tiny_windows_dont_crash(ui):
    ui.press("1")
    ui.wait("SERVER ON")
    for rows, cols in ((10, 20), (5, 5), (16, 60), (60, 200), (22, 80), (30, 110)):
        ui.screen.resize(rows, cols)
        ui.child.setwinsize(rows, cols)
        time.sleep(0.2)
        assert ui.alive()
    ui.wait("STOP SERVER")


def test_quit_keeps_things_running_and_reopening_picks_them_up(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    assert aim(env, "team", "plan").returncode == 0
    ui.wait("AGENTS 2/2")
    ui.press("q")
    ui.child.expect(pexpect.EOF, timeout=10)
    socket.create_connection(("127.0.0.1", PORT), 1).close()
    again = Console(env)
    again.wait("SERVER ON")
    again.wait("AGENTS 2/2")
    again.child.terminate(force=True)


def test_ctrl_c_leaves_the_server_running_and_restores_the_terminal(ui, env):
    ui.press("1")
    ui.wait("SERVER ON")
    ui.press("\x03")
    ui.child.expect(pexpect.EOF, timeout=10)
    socket.create_connection(("127.0.0.1", PORT), 1).close()
    assert "Server: running" in aim(env, "status").stdout


def test_buttons_never_move_when_state_changes(ui):
    def where(label):
        ui.pump()
        return [(y, line.find(label)) for y, line in enumerate(ui.screen.display) if label in line]

    before = {label: where(label) for label in ("ADD AGENT", "START MISSION", "VIEW AGENT")}
    ui.press("1")
    ui.wait("SERVER ON")
    after = {label: where(label) for label in before}
    assert before == after
