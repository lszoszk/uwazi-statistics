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


def render_embed(
    *,
    instance_url: str,
    types: list[str],
    chart_plan: list[dict],
    label_map: dict[str, str] | None = None,
    api_base: str | None = None,
    template_name: str = "embed.html.j2",
) -> str:
    """Render the *live* embed — config-only HTML; the browser does the
    aggregation fetch at load time and on URL state changes.

    `instance_url`  — shown in the topbar and footer as the data source.
    `api_base`      — what the JS prefixes onto `/api/search?...` when
                      fetching. Default = `instance_url`. Use an empty
                      string (or a `localhost` URL) when serving the
                      embed through a local proxy that strips the CORS
                      restriction Uwazi sets on its public API.

    The page contains no entity data at build time. That keeps the file
    tiny (~25 KB), shippable as an Uwazi page, and means the numbers are
    always current with the live instance.
    """
    env = _env()
    tpl = env.get_template(template_name)
    base = instance_url.rstrip("/")
    config = {
        "instance_url": base,
        "api_base": (api_base if api_base is not None else base).rstrip("/"),
        "types": list(types),
        "chart_plan": list(chart_plan),
        "label_map": dict(label_map or {}),
    }
    return tpl.render(
        instance_url=instance_url,
        config_json=json.dumps(config, ensure_ascii=False),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def write_dashboard(html: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
