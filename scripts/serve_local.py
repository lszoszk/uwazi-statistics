"""Local-only dev server: static files + transparent /api/* proxy.

Why this exists:
    The Uwazi instance ships
        cross-origin-resource-policy: same-origin
        cross-origin-opener-policy:   same-origin
    on its /api responses and never sets `Access-Control-Allow-Origin`.
    A browser served from `localhost:8910` therefore can't fetch the
    aggregations directly — every request becomes a "Load failed" in
    the dev console.

    This script flips the embed into a same-origin setup: it serves the
    static `output/` dir AND forwards `/api/...` to the live Uwazi
    instance from the *server* (where CORS doesn't apply). The browser
    sees a clean same-origin fetch and aggregations work.

Local-only. Never deploy this script — it would defeat Uwazi's CORS
posture and is wide open by design.

Usage:
    # 1. Build the embed with relative API calls
    python -m uwazi_charts.build --embed \\
        --instance https://upr-info-database.uwazi.io \\
        --types 5d8ce04361cde0408222e9a8 \\
        --api-base "" \\
        --out output/embed.html

    # 2. Start this proxy server
    python scripts/serve_local.py   # http://localhost:8910/embed.html
"""

from __future__ import annotations

import argparse
import http.server
import os
import socketserver
import sys
from pathlib import Path

import requests   # already in project deps via uwazi_charts/fetch.py

DEFAULT_REMOTE = "https://upr-info-database.uwazi.io"
DEFAULT_PORT = 8910
DEFAULT_ROOT = Path(__file__).resolve().parent.parent / "output"
PROXY_TIMEOUT_S = 30


def make_handler(remote: str, root: Path):
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(root), **kwargs)

        def log_message(self, fmt, *args):
            # Less spammy than the default — just method + path + status.
            sys.stderr.write(f"[{self.command}] {self.path}\n")

        def _proxy(self):
            upstream = remote.rstrip("/") + self.path
            try:
                r = requests.request(
                    self.command, upstream,
                    timeout=PROXY_TIMEOUT_S,
                    headers={"Accept": "application/json",
                             "User-Agent": "uwazi-statistics-local-proxy"},
                )
            except requests.RequestException as e:
                self.send_error(502, f"proxy error: {e}")
                return
            self.send_response(r.status_code)
            self.send_header(
                "Content-Type",
                r.headers.get("Content-Type", "application/json; charset=utf-8"),
            )
            self.send_header("Cache-Control", "no-store")
            # Emit ACAO even though we're same-origin — lets the user
            # open embed.html directly from disk if they want to.
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(r.content)

        def do_GET(self):    # noqa: N802
            if self.path.startswith("/api/"):
                return self._proxy()
            return super().do_GET()

        def do_HEAD(self):   # noqa: N802
            if self.path.startswith("/api/"):
                return self._proxy()
            return super().do_HEAD()

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser(description="Local dev server + Uwazi /api proxy")
    ap.add_argument("--remote", default=os.environ.get("UWAZI_URL", DEFAULT_REMOTE),
                    help="Upstream Uwazi instance to proxy /api/* to")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT,
                    help="Static file root (defaults to repo's output/)")
    args = ap.parse_args()

    if not args.root.exists():
        ap.error(f"static root {args.root} does not exist — run `python -m uwazi_charts.build --embed …` first")

    handler = make_handler(args.remote, args.root)
    # ThreadingMixIn so the API proxy doesn't block static file serving.
    class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
        daemon_threads = True
        allow_reuse_address = True

    with Server(("", args.port), handler) as httpd:
        print(f"serving  http://localhost:{args.port}/")
        print(f"  static {args.root}")
        print(f"  /api/* → {args.remote}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print()


if __name__ == "__main__":
    main()
