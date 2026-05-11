"""Jinja2 wrapper — turn (charts, meta) → static HTML."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render_dashboard(
    *,
    tabs: list[dict],
    instance_url: str,
    total_entities: int,
    template_name: str = "dashboard.html.j2",
) -> str:
    """Render the dashboard from a list of tabs.

    Each tab dict has shape:
        {"name": str, "slug": str, "count": int,
         "kpis": list[dict], "charts": list[dict]}

    The template renders one nav button per tab and one <section> pane
    per tab — only the first pane is visible on load; the JS swaps them.
    """
    env = _env()
    tpl = env.get_template(template_name)

    # Flatten every chart from every tab into a single JS payload so the
    # client can look them up by id when lazily initialising on tab activate.
    all_charts = [c for t in tabs for c in t["charts"]]
    return tpl.render(
        tabs=tabs,
        all_charts_json=json.dumps(all_charts, ensure_ascii=False),
        instance_url=instance_url,
        total_entities=total_entities,
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def write_dashboard(html: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
