"""Read counts straight from `/api/search?limit=0&aggregations=…`.

The Uwazi search endpoint returns Elasticsearch aggregations for every
filterable property on the requested type(s), shaped like:

    {
      "totalRows": N,
      "aggregations": {"all": {
        "<field_name>": {
          "doc_count": <total>,
          "buckets": [
            {"key": "<uuid>", "doc_count": <unfiltered>,
             "filtered": {"doc_count": <after_filters>},
             "label": "..."}  # label not always present
          ]
        },
        ...
      }}
    }

We never download row-level data — the buckets *are* the counts we'd
otherwise compute client-side, post-flatten. That is the whole point of
the embeddable dashboard: 117 k+ entities, ~50 KB JSON per call,
filter-aware out of the box.

Public surface:
    AggConfig            — instance URL, type IDs, language
    fetch_aggregations() — single HTTP call, returns parsed payload
    aggregations_to_charts() — payload + plan → list of chart dicts
    default_chart_plan() — sensible default plan from a schema-templates list

CLI:
    python -m uwazi_charts.aggregations --instance https://… --types <id>,<id>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Iterable

import requests

from uwazi_charts import charts as charts_mod
from uwazi_charts.fetch import (
    DEFAULT_LANGUAGE,
    DEFAULT_TIMEOUT_S,
    _get_with_retry,
    _make_session,
)

# Year-pattern fallback for session labels like "44 - November 2023".
_YEAR_RX = re.compile(r"\b(19|20)\d{2}\b")


@dataclass
class AggConfig:
    """Single-shot config for the aggregations endpoint."""
    instance_url: str
    types: list[str]                          # template _ids
    language: str = DEFAULT_LANGUAGE
    user_agent: str = "uwazi-charts-poc"
    filters: dict[str, list[str]] = field(default_factory=dict)
    # Optional pre-parsed RISON state from a Library URL — when provided,
    # overrides `types` + `filters` so callers can keep upstream URL shape.
    rison_state: dict | None = None


# ─────────────────────────────────────────────────────────────────────────────
# 1. HTTP — fetch the payload from /api/search
# ─────────────────────────────────────────────────────────────────────────────

def _flatten_filters_to_params(
    types: Iterable[str], filters: dict[str, list[str]]
) -> list[tuple[str, str]]:
    """Encode Uwazi-style search params as a list of (key, value) tuples.

    `requests` accepts `params=[(k, v), ...]` and emits repeated keys, which
    is what `types[]=…&filters[response][values][]=…` actually needs on the
    wire. URL-encoding `[`/`]` in keys is fine — Uwazi's validator decodes
    them before applying the JSON schema.
    """
    out: list[tuple[str, str]] = [("limit", "0")]
    for tid in types:
        out.append(("types[]", tid))
    for fname, values in (filters or {}).items():
        if not values:
            continue
        for v in values:
            out.append((f"filters[{fname}][values][]", v))
    return out


def fetch_aggregations(config: AggConfig) -> dict:
    """One GET to `/api/search?limit=0&...` — returns the parsed JSON."""
    session = _make_session(config.user_agent, config.language)
    types = config.types
    filters = config.filters
    if config.rison_state:
        # Lift types + filters out of the RISON state. Anything else (sort,
        # paging, includeUnpublished) is dropped — irrelevant for aggs.
        types = config.rison_state.get("types", types) or types
        rs_filters = config.rison_state.get("filters") or {}
        # Uwazi shape: filters: {field: {values: [...]} or {from: ..., to: ...}}
        filters = {
            k: v["values"] for k, v in rs_filters.items()
            if isinstance(v, dict) and v.get("values")
        }
    params = _flatten_filters_to_params(types, filters)
    r = _get_with_retry(
        session,
        f"{config.instance_url}/api/search",
        params=params,
    )
    return r.json()


# ─────────────────────────────────────────────────────────────────────────────
# 2. Schema → default plan (which charts to build for which fields)
# ─────────────────────────────────────────────────────────────────────────────

# Property-type → chart-kind mapping, copied from discover.py for symmetry.
_KIND_FOR_TYPE = {
    "select": "bar",
    "relationship": "bar",
    "multiselect": "bar_multi",
    # inherited fields surface as relationship (denormalised) — same chart
}


def default_chart_plan(
    schema_templates: list[dict],
    *,
    type_ids: Iterable[str] | None = None,
    max_charts: int = 12,
) -> list[dict]:
    """Build a default chart plan from `/api/templates`.

    Each plan entry: `{"field": str, "title": str, "kind": str}`.
    `kind` is one of: bar, bar_multi (multiselect — same JS rendering),
    line_year (sessions → year x-axis).
    """
    plan: list[dict] = []
    type_set = set(type_ids or [])
    seen_fields: set[str] = set()
    sessions_added = False

    for tpl in schema_templates:
        tid = tpl.get("_id") or tpl.get("id")
        if type_set and tid not in type_set:
            continue
        for prop in tpl.get("properties", []) or []:
            name = prop.get("name")
            ptype = prop.get("type")
            if not name or name in seen_fields:
                continue
            kind = _KIND_FOR_TYPE.get(ptype)
            if not kind:
                continue
            seen_fields.add(name)
            plan.append({
                "field": name,
                "title": prop.get("label") or _pretty(name),
                "kind": kind,
            })
            # Sessions are relationships — also fold a per-year line chart.
            if name == "session" and not sessions_added:
                plan.append({
                    "field": "session",
                    "title": "Recommendations per year",
                    "kind": "line_year",
                })
                sessions_added = True
    return plan[:max_charts]


def _pretty(name: str) -> str:
    parts = name.split("___")
    return " · ".join(p.replace("_", " ").strip().title() or p for p in parts)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Payload → list of chart dicts (the format the template already consumes)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_id(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s).strip("_").lower() or "chart"


def _bucket_counts(bucket: dict) -> int:
    """Prefer filtered.doc_count (post-filter) over doc_count (total). When
    the user has no filters applied, both are equal."""
    f = bucket.get("filtered")
    if isinstance(f, dict) and "doc_count" in f:
        return int(f["doc_count"])
    return int(bucket.get("doc_count", 0))


def _bucket_label(bucket: dict, fallback_map: dict[str, str] | None = None) -> str:
    """Some aggregations carry a `label` (selects, multiselects, relationships
    with denormalised labels); others (e.g. `_types`, `related_document`) only
    carry the `key`. `fallback_map` lets the caller pass a key→label dict from
    a separate lookup."""
    if bucket.get("label"):
        return str(bucket["label"])
    key = str(bucket.get("key", ""))
    if fallback_map and key in fallback_map:
        return fallback_map[key]
    return key


def _bar_chart_from_buckets(
    field: str, title: str, buckets: list[dict],
    *, top_n: int = 15, label_map: dict[str, str] | None = None,
) -> dict:
    """Top-N buckets → single-series bar chart dict.

    Buckets with 0 (post-filter) counts are dropped — that's what makes the
    chart filter-aware: a state with no matching recommendations disappears
    from its tab rather than rendering an empty bar.
    """
    pairs = [
        (_bucket_label(b, label_map), _bucket_counts(b))
        for b in buckets
    ]
    pairs = [(l, v) for l, v in pairs if v > 0]
    pairs.sort(key=lambda lv: -lv[1])
    if len(pairs) > top_n:
        head = pairs[:top_n]
        other = sum(v for _, v in pairs[top_n:])
        labels = [l for l, _ in head] + ["Other"]
        values = [v for _, v in head] + [int(other)]
    else:
        labels = [l for l, _ in pairs]
        values = [v for _, v in pairs]
    return {
        "id": _safe_id(field),
        "title": title,
        "kind": "bar",
        "labels": labels,
        "values": values,
        "n": int(sum(values)),
    }


def _line_year_from_session_buckets(
    field: str, title: str, buckets: list[dict],
    *, label_map: dict[str, str] | None = None,
) -> dict:
    """Session buckets → recommendations-per-year line chart.

    Session labels carry a year (e.g. "44 - November 2023"). We extract the
    first 4-digit year out of each label, sum buckets per year, and emit a
    sorted line series. Sessions whose label has no year parse-out are
    silently skipped (rare — would indicate a malformed session).
    """
    per_year: dict[int, int] = {}
    for b in buckets:
        label = _bucket_label(b, label_map)
        m = _YEAR_RX.search(label or "")
        if not m:
            continue
        y = int(m.group(0))
        per_year[y] = per_year.get(y, 0) + _bucket_counts(b)
    years = sorted(per_year.keys())
    return {
        "id": _safe_id(f"{field}_per_year"),
        "title": title,
        "kind": "line",
        "labels": years,
        "values": [per_year[y] for y in years],
        "n": int(sum(per_year.values())),
    }


def aggregations_to_charts(
    payload: dict,
    plan: list[dict],
    *,
    label_map: dict[str, str] | None = None,
) -> list[dict]:
    """Turn an `/api/search` payload into a list of chart dicts ready for the
    existing template. The plan controls which fields appear, with what
    title, and in what role.
    """
    aggs = (payload.get("aggregations") or {}).get("all") or {}
    out: list[dict] = []
    for entry in plan:
        fname = entry["field"]
        agg = aggs.get(fname)
        if not agg or not agg.get("buckets"):
            continue
        kind = entry.get("kind") or "bar"
        title = entry.get("title") or _pretty(fname)
        buckets = agg["buckets"]
        if kind == "line_year":
            out.append(_line_year_from_session_buckets(
                fname, title, buckets, label_map=label_map))
        else:
            out.append(_bar_chart_from_buckets(
                fname, title, buckets, label_map=label_map))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# CLI — sanity-print the chart deck a live build would emit
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--instance", default=os.environ.get("UWAZI_URL"),
                    help="Uwazi instance base URL (or set UWAZI_URL)")
    ap.add_argument("--types", default="",
                    help="Comma-separated template IDs to aggregate over")
    ap.add_argument("--rison",
                    help="Raw RISON `q` value from a Library URL (e.g. \"(...)\"). "
                         "When given, overrides --types and applies filters.")
    args = ap.parse_args()

    if not args.instance:
        ap.error("missing --instance (or UWAZI_URL)")

    type_ids = [t for t in args.types.split(",") if t.strip()]
    rison_state = None
    if args.rison:
        from uwazi_charts.rison import loads as rison_loads
        rison_state = rison_loads(args.rison)
        type_ids = type_ids or rison_state.get("types") or []

    config = AggConfig(
        instance_url=args.instance.rstrip("/"),
        types=type_ids,
        rison_state=rison_state,
    )
    payload = fetch_aggregations(config)
    print(f"totalRows = {payload.get('totalRows')}")
    print(f"agg field count = {len(payload.get('aggregations', {}).get('all', {}))}")
    print("first few buckets per field:")
    for fname, agg in (payload.get("aggregations") or {}).get("all", {}).items():
        if not isinstance(agg, dict) or "buckets" not in agg:
            continue
        bks = agg["buckets"][:3]
        print(f"  {fname}:")
        for b in bks:
            print(f"    {_bucket_label(b):40} {_bucket_counts(b):>7}")


if __name__ == "__main__":
    main()
