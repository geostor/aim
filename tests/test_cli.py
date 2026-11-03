"""aimserv, aimbot and aimwatch, on the test database, with a stand-in for claude."""

import os
import signal
import socket
import stat
import subprocess
import time
from pathlib import Path

import pytest

BIN = Path(__file__).parent.parent / "bin"
PORT = 18791

# Stands in for claude in streaming mode: answers its prompt and arguments, then exits.
FAKE_CLAUDE = """#!/usr/bin/env python3
import json, sys
line = sys.stdin.readline()
content = json.loads(line)["message"]["content"] if line else ""
text = content + "\\n" + "\\n".join(sys.argv[1:])
print(json.dumps({"type": "result", "subtype": "success", "result": text}), flush=True)
"""


@pytest.fixture
def env(tmp_path, test_database):
    fake = tmp_path / "claude"
    fake.write_text(FAKE_CLAUDE)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    return os.environ | {
        "AIM_RUNS": str(tmp_path / "runs"),
        "AIM_DB": "aim_test",
        "AIM_PORT": str(PORT),
        "AIM_CLAUDE": str(fake),
    }


def start_server(env):
    proc = subprocess.Popen(
        [BIN / "aimserv"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
    )
    deadline = time.time() + 20
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.2).close()
            return proc
        except OSError:
            if proc.poll() is not None or time.time() > deadline:
                raise RuntimeError(proc.stderr.read())
            time.sleep(0.1)


def stop_server(proc):
    proc.send_signal(signal.SIGINT)
    proc.wait(15)
    return proc.stderr.read()


def sql(statement):
    subprocess.run(
        ["podman", "exec", "aim-postgres", "psql", "-U", "aim", "-d", "aim_test", "-c", statement],
        check=True, capture_output=True,
    )


def add_message(text="hello"):
    sql(
        "INSERT INTO aim.users (name, signed_on, last_seen, signed_off) "
        "VALUES ('ann', now(), now(), now());"
        "INSERT INTO aim.events (time, kind, sender_id, sender, text, room) "
        f"SELECT now(), 'message', max(id), 'ann', {text}, '#r' FROM aim.users"
    )


def watch_once(env):
    return subprocess.run(
        [BIN / "aimwatch", "-o"], env=env, capture_output=True, text=True, timeout=30
    )


def test_aimserv_runs_in_the_foreground_until_interrupted(env):
    proc = start_server(env)
    assert proc.poll() is None
    stop_server(proc)
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", PORT), 0.2)


def test_aimserv_saves_the_old_conversation_before_wiping(env, tmp_path):
    proc = start_server(env)
    add_message("'keep me'")
    stop_server(proc)
    proc = start_server(env)
    try:
        saved = list((tmp_path / "runs").glob("*.tsv"))
        assert len(saved) == 1 and "keep me" in saved[0].read_text()
        assert watch_once(env).stdout == ""
    finally:
        stop_server(proc)


def test_aimserv_refuses_a_port_someone_else_holds(env):
    with socket.socket() as other:
        other.bind(("127.0.0.1", PORT))
        other.listen()
        result = subprocess.run(
            [BIN / "aimserv"], env=env, capture_output=True, text=True, timeout=30
        )
    assert result.returncode != 0 and "in use" in result.stderr


def test_aimbot_passes_mission_role_and_limits_to_claude(env):
    result = subprocess.run(
        [BIN / "aimbot", "ann", "You are friendly."],
        env=env, input="Do the thing.", capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "Do the thing.\n\nYou are ann. You are friendly." in result.stdout
    args = result.stdout.splitlines()
    assert args[args.index("--tools") + 1] == "WebSearch,WebFetch"
    assert args[args.index("--allowedTools") + 1] == "mcp__aim,WebSearch,WebFetch"
    assert "--strict-mcp-config" in args and "--max-budget-usd" in args
    assert f"http://127.0.0.1:{PORT}/mcp" in result.stdout


def test_aimbot_can_be_chat_only(env):
    result = subprocess.run(
        [BIN / "aimbot", "ann", "role"],
        env=env | {"AIM_TOOLS": ""}, input="m", capture_output=True, text=True, timeout=30,
    )
    args = result.stdout.splitlines()
    assert args[args.index("--tools") + 1] == ""
    assert args[args.index("--allowedTools") + 1] == "mcp__aim"


def test_aimbot_needs_a_name_role_and_mission(env):
    assert subprocess.run([BIN / "aimbot", "ann"], env=env, capture_output=True).returncode == 2
    empty = subprocess.run(
        [BIN / "aimbot", "ann", "role"], env=env, input="", capture_output=True, text=True
    )
    assert empty.returncode != 0 and "empty" in empty.stderr


def test_aimwatch_prints_clean_tab_separated_lines(env):
    proc = start_server(env)
    try:
        add_message(r"E'two\nlines \033[31mred\ttab'")
        lines = watch_once(env).stdout.splitlines()
        assert len(lines) == 1
        assert lines[0].split("\t")[1:] == ["message", "ann", "#r", "two lines  [31mred tab"]
    finally:
        stop_server(proc)


def test_aimwatch_follows_new_events_and_exits_when_its_reader_does(env):
    proc = start_server(env)
    watcher = subprocess.Popen(
        f"{BIN / 'aimwatch'} | head -1", shell=True, env=env,
        stdout=subprocess.PIPE, text=True,
    )
    try:
        time.sleep(1.5)
        add_message("'live'")
        time.sleep(1.5)
        add_message("'next write finds the pipe closed'")
        output, _ = watcher.communicate(timeout=20)
        assert "live" in output
    finally:
        watcher.kill()
        stop_server(proc)


# --- the aim menu and its subcommands -----------------------------------------


@pytest.fixture
def aim_env(env, tmp_path):
    fake = tmp_path / "claude"
    fake.write_text('#!/bin/sh\ntrap "exit 0" TERM\nsleep 30 & wait\necho the final answer\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    environ = env | {"AIM_STATE": str(tmp_path / "state"), "AIM_CLAUDE": str(fake)}
    yield environ
    subprocess.run([BIN / "aim", "stop"], env=environ, capture_output=True, timeout=30)


def aim(env, *args):
    return subprocess.run(
        [BIN / "aim", *args], env=env, capture_output=True, text=True, timeout=60
    )


def test_aim_starts_and_stops_the_server(aim_env):
    started = aim(aim_env, "start")
    assert started.returncode == 0, started.stderr
    socket.create_connection(("127.0.0.1", PORT), 1).close()
    assert "already running" in aim(aim_env, "start").stdout
    assert "Server: running" in aim(aim_env, "status").stdout
    assert aim(aim_env, "stop").returncode == 0
    assert "Server: stopped" in aim(aim_env, "status").stdout


def test_aim_reports_a_failed_start(aim_env):
    with socket.socket() as other:
        other.bind(("127.0.0.1", PORT))
        other.listen()
        result = aim(aim_env, "start")
    assert result.returncode != 0 and "failed to start" in result.stderr
    assert "Server: stopped" in aim(aim_env, "status").stdout


def test_aim_adds_and_removes_agents(aim_env):
    assert "start it first" in aim(aim_env, "add", "ann", "friendly", "hello").stderr
    aim(aim_env, "start")
    added = aim(aim_env, "add", "ann", "friendly", "hello")
    assert added.returncode == 0, added.stderr
    assert "ann: running" in aim(aim_env, "status").stdout
    assert "already running" in aim(aim_env, "add", "ann", "friendly", "hello").stderr
    assert aim(aim_env, "rm", "ann").returncode == 0
    assert "Agents: none" in aim(aim_env, "status").stdout
    assert aim(aim_env, "rm", "ann").returncode != 0


def test_aim_starts_a_team_and_stop_ends_it(aim_env):
    aim(aim_env, "start")
    assert aim(aim_env, "team", "hello").returncode == 0
    status = aim(aim_env, "status").stdout
    assert "ann: running" in status and "ben: running" in status
    aim(aim_env, "stop")
    assert "Agents: none" in aim(aim_env, "status").stdout


def test_aim_rejects_bad_names_and_missions(aim_env):
    aim(aim_env, "start")
    assert "invalid name" in aim(aim_env, "add", "a';DROP", "role", "hello").stderr
    assert "invalid name" in aim(aim_env, "add", "../x", "role", "hello").stderr
    assert "no such mission" in aim(aim_env, "add", "ann", "role", "../hello").stderr


def test_removing_an_agent_frees_its_screen_name(aim_env):
    aim(aim_env, "start")
    sql(
        "INSERT INTO aim.users (name, token_hash, signed_on, last_seen) "
        "VALUES ('ann', 'x', now(), now())"
    )
    aim(aim_env, "add", "ann", "friendly", "hello")
    aim(aim_env, "rm", "ann")
    row = subprocess.run(
        ["podman", "exec", "aim-postgres", "psql", "-U", "aim", "-d", "aim_test", "-At", "-c",
         "SELECT now() - last_seen > interval '1 hour' FROM aim.users WHERE name = 'ann'"],
        capture_output=True, text=True, check=True,
    )
    assert row.stdout.strip() == "t"


@pytest.fixture
def test_list_shows_each_agents_latest_message(aim_env):
    aim(aim_env, "start")
    aim(aim_env, "add", "ann", "friendly", "hello")
    aim(aim_env, "add", "ben", "terse", "hello")
    add_message("'first'")
    add_message(r"E'latest\tmessage\nhere'")
    fields = [line.split("\t") for line in aim(aim_env, "list").stdout.splitlines()]
    assert [f[:2] for f in fields] == [["ann", "running"], ["ben", "running"]]
    assert fields[0][3] == "latest message here"  # newest, on one line
    assert fields[1][2:] == ["-", "-"]  # ben hasn't said anything
    status = aim(aim_env, "status").stdout
    assert "latest message here" in status and "no messages yet" in status


def test_viewing_an_agent_shows_its_messages_then_its_summary(aim_env):
    aim(aim_env, "start")
    aim(aim_env, "add", "ann", "friendly", "hello")
    add_message("'from ann'")
    sql(
        "INSERT INTO aim.users (name, signed_on, last_seen, signed_off) "
        "VALUES ('ben', now(), now(), now());"
        "INSERT INTO aim.events (time, kind, sender_id, sender, text, room) "
        "SELECT now(), 'message', max(id), 'ben', 'from ben', '#r' FROM aim.users"
    )
    running = aim(aim_env, "show", "ann").stdout
    assert "from ann" in running and "from ben" not in running
    assert "still running" in running

    log = Path(aim_env["AIM_STATE"]) / "agents" / "ann.log"
    log.write_text("the closing summary\n")
    finished = aim(aim_env, "show", "ann").stdout
    assert "from ann" in finished and "the closing summary" in finished




def test_every_agent_gets_the_team_rules_with_its_room_and_roster(env, tmp_path):
    result = subprocess.run(
        [BIN / "aimbot", "ann", "You are friendly."],
        env=env | {"AIM_ROOM": "#plan", "AIM_TEAM": "lead,ann,ben"},
        input="Do the thing.", capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "Do the thing.\n\nYou are ann. You are friendly." in result.stdout
    assert "exactly this screen name: ann" in result.stdout
    assert "Join #plan" in result.stdout
    assert "The team is: lead, ann, ben. The lead is lead." in result.stdout
    assert "delivered to you automatically" in result.stdout and "FINAL" in result.stdout
    assert "{" not in result.stdout.split("--model")[0]
    assert "stream-json" in result.stdout


def test_aim_starts_agents_in_one_room_with_the_lead_first(aim_env, tmp_path):
    missions = tmp_path / "missions"
    (missions / "Big-Plan").mkdir(parents=True)
    (missions / "Big-Plan" / "mission.md").write_text("goal\n")
    (missions / "Big-Plan" / "team.tsv").write_text("# comment\nboss\trole\nann\trole\n")
    fake = Path(aim_env["AIM_CLAUDE"])
    fake.write_text(FAKE_CLAUDE)
    env = aim_env | {"AIM_MISSIONS": str(missions)}
    aim(env, "start")
    assert aim(env, "team", "Big-Plan").returncode == 0
    assert aim(env, "add", "extra", "role", "Big-Plan").returncode == 0
    time.sleep(1)
    agents = Path(env["AIM_STATE"]) / "agents"
    ann = (agents / "ann.log").read_text()
    assert "Join #big-plan" in ann and "The team is: boss, ann. The lead is boss." in ann
    assert "The team is: boss, ann, extra. The lead is boss." in (agents / "extra.log").read_text()
    shown = aim(env, "protocol", "Big-Plan").stdout
    assert "Join #big-plan" in shown and "The lead is boss." in shown and "{" not in shown


def test_agents_are_confined_to_the_missions_folder_and_only_some_can_write(aim_env, tmp_path):
    missions = tmp_path / "missions"
    (missions / "site").mkdir(parents=True)
    (missions / "site" / "mission.md").write_text("goal\n")
    (missions / "site" / "team.tsv").write_text("boss\tleads\nmaker\tbuilds\tWrite,Edit\n")
    Path(aim_env["AIM_CLAUDE"]).write_text(FAKE_CLAUDE)
    env = aim_env | {"AIM_MISSIONS": str(missions)}
    aim(env, "start")
    assert aim(env, "team", "site").returncode == 0
    time.sleep(1.5)
    agents = Path(env["AIM_STATE"]) / "agents"

    def args(name):
        return (agents / f"{name}.log").read_text().splitlines()

    for name in ("boss", "maker"):
        assert "--restricted" in args(name)
    assert args("boss")[args("boss").index("--tools") + 1] == "WebSearch,WebFetch,Read,Grep,Glob"
    assert args("maker")[args("maker").index("--tools") + 1] == "WebSearch,WebFetch,Read,Grep,Glob,Write,Edit"
    assert "Write" in args("maker")[args("maker").index("--allowedTools") + 1]
    assert (missions / "site" / "output").is_dir()


def test_viewer_serves_only_the_page_refreshes_and_blocks_outside_requests(env, tmp_path):
    import urllib.request

    missions = tmp_path / "missions"
    (missions / "site" / "output").mkdir(parents=True)
    (missions / "site" / "mission.md").write_text("goal\n")
    (missions / "secret.txt").write_text("private")
    viewer_env = env | {"AIM_MISSIONS": str(missions), "AIM_VIEW_PORT": "18797", "AIM_VIEW_NO_OPEN": "1"}
    viewer = subprocess.Popen([BIN / "aimview", "site"], env=viewer_env, stdout=subprocess.DEVNULL)

    def get(path):
        with urllib.request.urlopen(f"http://127.0.0.1:18797{path}", timeout=5) as reply:
            return reply.read().decode(), reply.headers

    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", 18797), 0.1).close()
                break
            except OSError:
                time.sleep(0.1)
        body, headers = get("/")
        assert "Nothing built yet" in body and "__version" in body
        policy = headers["Content-Security-Policy"]
        assert "default-src 'none'" in policy and "connect-src 'self'" in policy
        assert get("/__version")[0] == "none"

        (missions / "site" / "output" / "index.html").write_text("<h1>The page</h1>")
        assert "The page" in get("/")[0]
        first = get("/__version")[0]
        time.sleep(0.01)
        (missions / "site" / "output" / "index.html").write_text("<h1>Changed page</h1>")
        assert get("/__version")[0] != first

        assert "private" not in get("/../secret.txt")[0]  # any other path is just the page
    finally:
        viewer.terminate()
        viewer.wait(5)


def test_camera_photographs_each_save_reports_problems_and_tells_the_room(env, tmp_path):
    missions = tmp_path / "missions"
    site = missions / "site"
    (site / "output").mkdir(parents=True)
    (site / "mission.md").write_text("goal\n")
    (site / "team.tsv").write_text("maker\tbuilds\tWrite\n")
    page = site / "output" / "index.html"
    page.write_text('<!doctype html><meta name="viewport" content="width=device-width"><h1>First</h1>')
    environ = env | {"AIM_MISSIONS": str(missions)}
    server = start_server(environ)
    camera = subprocess.Popen([BIN / "aimshot", "site"], env=environ, stdout=subprocess.DEVNULL)
    shots = site / "output" / "screenshots"

    def wait_for(check, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if check():
                return
            time.sleep(0.2)
        raise AssertionError(f"timed out; report: {(shots / 'report.txt').exists()}")

    def report():
        path = shots / "report.txt"
        return path.read_text() if path.exists() else ""

    try:
        wait_for(lambda: (shots / "phone.png").exists() and "No problems" in report())
        for name in ("desktop.png", "phone.png"):
            assert (shots / name).read_bytes()[:4] == b"\x89PNG"
        wait_for(lambda: "New screenshots" in watch_once(environ).stdout)
        line = next(l for l in watch_once(environ).stdout.splitlines() if "New screenshots" in l)
        assert line.split("\t")[2:4] == ["camera", "#site"]

        first = (shots / "desktop.png").read_bytes()
        page.write_text(
            "<!doctype html><body style='background:red'><div style='width:3000px'>wide</div>"
            "<script>fetch('https://example.com/steal');throw new Error('boom')</script>"
        )
        wait_for(lambda: "boom" in report())
        text = report()
        assert "script error" in text and "boom" in text
        assert "sideways" in text
        assert "example.com" in text  # outside requests are caught, not made
        assert (shots / "desktop.png").read_bytes() != first
    finally:
        camera.terminate()
        camera.wait(15)
        stop_server(server)


def test_camera_only_starts_when_someone_can_write(aim_env, tmp_path):
    missions = tmp_path / "missions"
    (missions / "talk").mkdir(parents=True)
    (missions / "talk" / "mission.md").write_text("goal\n")
    (missions / "talk" / "team.tsv").write_text("ann\trole\n")
    env = aim_env | {"AIM_MISSIONS": str(missions)}
    aim(env, "start")
    aim(env, "team", "talk")
    assert not (Path(env["AIM_STATE"]) / "camera.pid").exists()
