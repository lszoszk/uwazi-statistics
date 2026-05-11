"""Build the static dashboard from a cached parquet (or synthetic sample).

Usage:

    python -m uwazi_charts.build --sample              # uses fixture data
    python -m uwazi_charts.build                       # reads cache/entities.parquet
    python -m uwazi_charts.build --from path/to.parquet --out output/dash.html
"""

from __future__ import annotations

import argparse
import os
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


def _prepare(df: pd.DataFrame, profile: dict) -> pd.DataFrame:
    """Flatten the metadata + add a year column. Idempotent."""
    df = flatten.add_year_column(df, src="creationDate", dst="year")
    for col, _ in profile.get("categorical", []):
        df = flatten.add_label_column(df, col, multi=False)
    for col, _ in profile.get("multi", []):
        df = flatten.add_label_column(df, col, multi=True)
    return df


def build_html_from_df(
    df: pd.DataFrame,
    *,
    instance_url: str,
    profile: dict | None = None,
    schema_templates: list[dict] | None = None,
) -> str:
    """Render the full dashboard HTML.

    Profile resolution order:
      1. Explicit `profile` arg (caller overrides everything)
      2. Hybrid: schema-driven (from `schema_templates`) merged with
         DataFrame-inferred (which is the only place denormalised
         inherited fields like `foo___bar` appear)
      3. DataFrame-only inference
      4. Hard-coded `UPR_INFO_DEFAULTS` (fixture / no metadata at all)
    """
    if profile is None:
        df_profile = discover.discover_profile_from_df(df)
        if schema_templates:
            schema_fields = discover.parse_templates(schema_templates)
            schema_profile = discover.build_profile(schema_fields, max_charts=24)
            # Schema first so authoritative labels win; DF adds inherited fields.
            profile = discover.merge_profiles(schema_profile, df_profile, max_charts=12)
        elif any(df_profile.get(k) for k in ("categorical", "multi", "date")):
            profile = df_profile
        else:
            profile = UPR_INFO_DEFAULTS

    df = _prepare(df, profile)
    chart_list = charts_mod.auto_charts_from_df(
        df,
        categorical=profile.get("categorical", []),
        multi=profile.get("multi", []),
        has_year=True,
    )
    kpis = charts_mod.compute_kpis(df)
    return render_dashboard(
        charts=chart_list,
        instance_url=instance_url,
        total_entities=int(df.shape[0]),
        kpis=kpis,
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
