"""Auto-discover a chart profile from an Uwazi instance's `/api/templates`.

Uwazi property types we care about:

    select          → 1 label, low cardinality   → bar chart
    multiselect     → many labels                → bar chart (explode)
    relationship    → like select/multiselect    → bar chart
    date / multidate → ms-epoch values           → year line chart
    daterange       → start/end pair             → ignored (would need its own viz)
    numeric         → free numbers               → histogram (TODO — skip in v1)
    geolocation     → lat/lng pair               → map (TODO — skip in v1)
    text / markdown → free text                  → IGNORED (we don't chart content)
    link / image / media / preview → IGNORED

The output `profile` dict is what `build.build_html_from_df()` consumes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

# Property types we know how to chart, in priority order
CATEGORICAL_TYPES = {"select", "relationship"}
MULTI_TYPES = {"multiselect"}
DATE_TYPES = {"date", "multidate"}
SKIP_TYPES = {"text", "markdown", "link", "image", "media", "preview",
              "numeric", "geolocation", "daterange", "media", "image"}

# Hide system properties Uwazi auto-adds — they're rarely useful charts
SYSTEM_PROPERTIES = {"text_search", "creationDate", "editDate"}


def pretty_label(name: str) -> str:
    """Turn an Uwazi property name into a readable card title.

    Inherited (denormalised) relationship fields use a triple-underscore
    separator: `state_under_review___regional_group`. Split those on the
    separator first, then format each segment, then join with " · " so the
    parent/child relationship reads naturally.
    """
    parts = name.split("___")
    return " · ".join(
        seg.replace("_", " ").strip().title() or seg for seg in parts
    )


@dataclass(frozen=True)
class Field:
    name: str           # property `name` (JSON key in metadata)
    label: str          # property `label` (human title)
    type: str           # property `type` from Uwazi
    template_id: str
    template_name: str


def parse_templates(templates: list[dict]) -> list[Field]:
    """Flatten Uwazi's templates list into a flat Field list."""
    fields: list[Field] = []
    for t in templates:
        tname = t.get("name") or "unnamed"
        tid = t.get("_id") or t.get("id") or ""
        for prop in t.get("properties") or []:
            name = prop.get("name")
            if not name or name in SYSTEM_PROPERTIES:
                continue
            fields.append(Field(
                name=name,
                label=prop.get("label") or pretty_label(name),
                type=prop.get("type") or "text",
                template_id=tid,
                template_name=tname,
            ))
    return fields


def build_profile(fields: Iterable[Field], *, max_charts: int = 12) -> dict:
    """Group fields by what kind of chart they suit, deduping by name.

    Returns the dict shape that `build.build_html_from_df()` expects:

        {"categorical": [(name, label), ...],
         "multi":       [(name, label), ...],
         "date":        [(name, label), ...]}     # for line charts

    Fields with the same `name` across multiple templates are merged (the
    first seen wins for the title — typical Uwazi convention).
    """
    seen: set[str] = set()
    categorical: list[tuple[str, str]] = []
    multi: list[tuple[str, str]] = []
    date: list[tuple[str, str]] = []

    for f in fields:
        if f.name in seen:
            continue
        seen.add(f.name)
        if f.type in CATEGORICAL_TYPES:
            categorical.append((f.name, f.label))
        elif f.type in MULTI_TYPES:
            multi.append((f.name, f.label))
        elif f.type in DATE_TYPES:
            date.append((f.name, f.label))

    # Trim to `max_charts` — keep categorical/multi balanced, then dates last.
    chartable: list[tuple[str, str, str]] = (
        [("categorical", n, l) for n, l in categorical]
        + [("multi", n, l) for n, l in multi]
        + [("date", n, l) for n, l in date]
    )
    chartable = chartable[:max_charts]

    return {
        "categorical": [(n, l) for kind, n, l in chartable if kind == "categorical"],
        "multi":       [(n, l) for kind, n, l in chartable if kind == "multi"],
        "date":        [(n, l) for kind, n, l in chartable if kind == "date"],
    }


def discover_profile_from_df(df: pd.DataFrame, *, max_charts: int = 12) -> dict:
    """Fallback when /api/templates isn't reachable: infer field types by
    looking at one populated metadata row. Less accurate (can't tell
    select vs multiselect with a single value) but works offline.

    Heuristic:
      - If the value list has > 1 item ever, treat as multi
      - If items carry `label`, treat as categorical; else as value-only
    """
    if "metadata" not in df.columns or df.empty:
        return {"categorical": [], "multi": [], "date": []}

    field_kinds: dict[str, str] = {}
    field_labels: dict[str, str] = {}

    for meta in df["metadata"]:
        if not isinstance(meta, dict):
            continue
        for key, items in meta.items():
            if key in SYSTEM_PROPERTIES or not items:
                continue
            n_items = len(items) if isinstance(items, list) else 1
            has_label = (isinstance(items, list) and items
                         and isinstance(items[0], dict) and items[0].get("label"))
            current = field_kinds.get(key)
            if has_label:
                if current is None or current == "categorical":
                    field_kinds[key] = "multi" if n_items > 1 else "categorical"
                # if it was "multi" once, stay multi
            elif current is None:
                # value-only — likely a text/numeric, skip in v1
                field_kinds[key] = "skip"
            field_labels.setdefault(key, pretty_label(key))

    categorical = [(k, field_labels[k]) for k, v in field_kinds.items() if v == "categorical"]
    multi = [(k, field_labels[k]) for k, v in field_kinds.items() if v == "multi"]
    out = {"categorical": categorical[:max_charts // 2],
           "multi": multi[:max_charts // 2],
           "date": []}
    return out


def merge_profiles(*profiles: dict, max_charts: int = 12) -> dict:
    """Merge multiple profile dicts (e.g. schema-driven + DF-inferred).

    Earlier profiles take precedence for labels — pass the authoritative
    one first. Duplicate field names are kept only on first occurrence.
    Trims to `max_charts` keeping a rough balance: categorical ≥ multi ≥ date.
    """
    seen: set[str] = set()
    cat: list[tuple[str, str]] = []
    multi: list[tuple[str, str]] = []
    date: list[tuple[str, str]] = []
    for p in profiles:
        for n, l in p.get("categorical", ()):
            if n not in seen:
                seen.add(n); cat.append((n, l))
        for n, l in p.get("multi", ()):
            if n not in seen:
                seen.add(n); multi.append((n, l))
        for n, l in p.get("date", ()):
            if n not in seen:
                seen.add(n); date.append((n, l))
    # Distribute the cap proportionally — at least 1 of each if present.
    return _trim_profile(cat, multi, date, max_charts)


def _trim_profile(cat, multi, date, max_charts):
    """Trim to max_charts, interleaving so we don't run out of one kind."""
    combined = []
    iters = [iter(cat), iter(multi), iter(date)]
    keep = [True, True, True]
    while any(keep) and len(combined) < max_charts:
        for i, it in enumerate(iters):
            if not keep[i]:
                continue
            try:
                combined.append((i, next(it)))
                if len(combined) >= max_charts:
                    break
            except StopIteration:
                keep[i] = False
    return {
        "categorical": [v for k, v in combined if k == 0],
        "multi":       [v for k, v in combined if k == 1],
        "date":        [v for k, v in combined if k == 2],
    }
