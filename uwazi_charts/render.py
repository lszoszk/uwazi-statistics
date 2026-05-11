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
    charts: list[dict],
    instance_url: str,
    total_entities: int,
    kpis: list[dict] | None = None,
    template_name: str = "dashboard.html.j2",
) -> str:
    env = _env()
    tpl = env.get_template(template_name)
    return tpl.render(
        charts=charts,
        charts_json=json.dumps(charts, ensure_ascii=False),
        instance_url=instance_url,
        total_entities=total_entities,
        kpis=kpis or [],
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def write_dashboard(html: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
