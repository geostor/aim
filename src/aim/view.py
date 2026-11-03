"""A local viewer for the page a team is building, which refreshes when the file changes.

It serves only that one file. The page was written by agents, so a content security
policy stops it loading anything from elsewhere or sending anything anywhere, which
also shows up any page that isn't truly self-contained.
"""

from __future__ import annotations

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

POLICY = (
    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
    "img-src data:; font-src data:; media-src data:; connect-src 'self'; "
    "form-action 'self'; base-uri 'none'"
)
WAITING = (
    "<!doctype html><title>AIM</title><body style='font:20px sans-serif;margin:3em'>"
    "<h1>Nothing built yet</h1><p>This page will appear here as soon as the team saves it.</p>"
)
REFRESH = """<script>
(() => {
  let seen = null;
  setInterval(async () => {
    try {
      const version = await (await fetch('/__version')).text();
      if (seen !== null && version !== seen) location.reload();
      seen = version;
    } catch (e) {}
  }, 1000);
})();
</script>"""


def version(page: Path) -> str:
    try:
        stat = page.stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"
    except OSError:
        return "none"


def render(page: Path) -> str:
    try:
        html = page.read_text(errors="replace")
    except OSError:
        html = WAITING
    return html + REFRESH


def make_handler(page: Path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            body = version(page) if path == "/__version" else render(page)
            kind = "text/plain" if path == "/__version" else "text/html; charset=utf-8"
            data = body.encode("utf-8", "replace")
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Content-Security-Policy", POLICY)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            self.do_GET()

        def log_message(self, *args):
            pass

    return Handler


def main() -> None:
    page, port = Path(sys.argv[1]), int(sys.argv[2])
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(page))
    print(f"http://127.0.0.1:{port}/", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
