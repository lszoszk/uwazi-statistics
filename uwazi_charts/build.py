"""Build the static dashboard from a cached parquet (or synthetic sample).

Usage:

    python -m uwazi_charts.build --sample              # uses fixture data
    python -m uwazi_charts.build                       # reads cache/entities.parquet
    python -m uwazi_charts.build --from path/to.parquet --out output/dash.html
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import pandas as pd

from uwazi_charts import charts as charts_mod
from uwazi_charts import discover, flatten
from uwazi_charts.fetch import FetchConfig, fetch_templates, load_cache
from uwazi_charts.render import render_dashboard, write_dashboard

# Fallback profile if /api/templates is unreachable AND no fields are found
# by the schema-less inference. Tuned to the UPR Info Database shape so the
# `--sample` and "first-run-against-UPR" flows produce something on screen.
UPR_INFO_DEFAULTS = {
    "categorical": [("regional_group", "Regional group")],
    "multi":       [("organisations", "Organisations"),
                    ("country_code",  "Country code")],
    "date":        [],
}


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-") or "tab"


def _prepare(df: pd.DataFrame, profile: dict) -> pd.DataFrame:
    """Flatten the metadata + add a year column. Idempotent."""
    df = flatten.add_year_column(df, src="creationDate", dst="year")
    for col, _ in profile.get("categorical", []):
        df = flatten.add_label_column(df, col, multi=False)
    for col, _ in profile.get("multi", []):
        df = flatten.add_label_column(df, col, multi=True)
    return df


def _resolve_profile(
    df: pd.DataFrame,
    schema_templates: list[dict] | None,
    *,
    max_charts: int = 12,
) -> dict:
    """Pick the best profile available for `df`.

    Profile resolution order:
      1. Hybrid: schema-driven (authoritative labels) + DF-inferred
         (only place inherited `foo___bar` fields appear)
      2. DataFrame-only inference
      3. Hard-coded UPR_INFO_DEFAULTS (fixture / no metadata at all)
    """
    df_profile = discover.discover_profile_from_df(df)
    if schema_templates:
        schema_fields = discover.parse_templates(schema_templates)
        schema_profile = discover.build_profile(schema_fields, max_charts=max_charts * 2)
        return discover.merge_profiles(schema_profile, df_profile, max_charts=max_charts)
    if any(df_profile.get(k) for k in ("categorical", "multi", "date")):
        return df_profile
    return UPR_INFO_DEFAULTS


def _build_tab(
    df: pd.DataFrame,
    *,
    name: str,
    slug: str,
    schema_templates: list[dict] | None,
) -> dict:
    """Produce one tab's worth of charts + KPIs from a (possibly filtered)
    DataFrame. Discovery + flatten + chart generation all run against the
    given subset so per-template tabs only show fields actually present."""
    profile = _resolve_profile(df, schema_templates)
    prepared = _prepare(df, profile)
    return {
        "name": name,
        "slug": slug,
        "count": int(prepared.shape[0]),
        "kpis": charts_mod.compute_kpis(prepared),
        "charts": [
            # Namespace chart ids per tab so two tabs charting the same field
            # don't collide on the canvas DOM id.
            {**c, "id": f"{slug}-{c['id']}"}
            for c in charts_mod.auto_charts_from_df(
                prepared,
                categorical=profile.get("categorical", []),
                multi=profile.get("multi", []),
                has_year=True,
            )
        ],
    }


def _template_id_to_name(schema_templates: list[dict] | None) -> dict[str, str]:
    if not schema_templates:
        return {}
    out = {}
    for t in schema_templates:
        tid = t.get("_id") or t.get("id")
        if tid:
            out[tid] = t.get("name") or "unnamed"
    return out


def build_html_from_df(
    df: pd.DataFrame,
    *,
    instance_url: str,
    schema_templates: list[dict] | None = None,
) -> str:
    """Render the full dashboard HTML.

    Always produces at least one tab ("All"). When multiple `template` IDs
    are present in the DataFrame AND the schema is available to resolve
    them to human names, adds one tab per template (most populous first)
    so each chart set is semantically clean.
    """
    tabs: list[dict] = [_build_tab(df, name="All", slug="all", schema_templates=schema_templates)]

    if "template" in df.columns:
        id_to_name = _template_id_to_name(schema_templates)
        counts = df["template"].value_counts()
        # Only add per-template tabs when we have multiple templates AND we
        # can give them readable names from the schema.
        if len(counts) > 1 and id_to_name:
            seen_slugs = {"all"}
            for tpl_id, _count in counts.items():
                name = id_to_name.get(tpl_id) or tpl_id[:8]
                slug = _slug(name)
                # Slug collisions are unlikely but possible — disambiguate.
                if slug in seen_slugs:
                    slug = f"{slug}-{tpl_id[:6]}"
                seen_slugs.add(slug)
                sub_df = df[df["template"] == tpl_id]
                tabs.append(_build_tab(
                    sub_df, name=name, slug=slug, schema_templates=schema_templates))

    return render_dashboard(
        tabs=tabs,
        instance_url=instance_url,
        total_entities=int(df.shape[0]),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--from", dest="from_path", type=Path,
                     default=Path("cache/entities.parquet"))
    src.add_argument("--sample", action="store_true",
                     help="Use synthetic fixture data (no network, no cache needed)")
    ap.add_argument("--out", type=Path, default=Path("output/index.html"))
    ap.add_argument("--instance", default=os.environ.get("UWAZI_URL", "https://example.org"),
                    help="Source instance URL (for schema fetch + footer)")
    ap.add_argument("--no-schema", action="store_true",
                    help="Skip /api/templates fetch; use DataFrame inference only")
    args = ap.parse_args()

    if args.sample:
        # Lazy import so the runtime dep on tests/ isn't required for the
        # normal flow.
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from tests.fixtures.upr_info_sample import make_sample  # noqa: E402
        df = make_sample(200)
        instance = "https://upr-info-database.uwazi.io (sample fixture)"
        schema_templates = None
    else:
        if not args.from_path.exists():
            ap.error(
                f"cache not found at {args.from_path}.\n"
                "Run `python -m uwazi_charts.fetch` first, or pass --sample."
            )
        df = load_cache(args.from_path)
        instance = args.instance

        # Try to pull the live schema so chart titles use the authoritative
        # `label` from /api/templates rather than name-mangled defaults.
        # Silent fallback to DF-only inference if the network is down.
        schema_templates = None
        if args.instance and not args.no_schema:
            try:
                schema_templates = fetch_templates(FetchConfig(
                    instance_url=args.instance.rstrip("/"),
                    user_agent=os.environ.get("UWAZI_USER_AGENT", "uwazi-charts-poc"),
                ))
                print(f"[build] fetched schema: {len(schema_templates)} templates")
            except Exception as e:
                print(f"[build] schema fetch failed ({e}), falling back to DF inference")

    html = build_html_from_df(df, instance_url=instance, schema_templates=schema_templates)
    write_dashboard(html, args.out)
    print(f"[build] {df.shape[0]:,} rows → {args.out}")


if __name__ == "__main__":
    main()
