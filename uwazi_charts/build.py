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
from uwazi_charts import flatten
from uwazi_charts.fetch import load_cache
from uwazi_charts.render import render_dashboard, write_dashboard

# Field → human title for the UPR Info Database. Easy to override via CLI later
# or auto-derive from `/api/templates` once we wire schema discovery in.
UPR_INFO_DEFAULTS = {
    "categorical": [
        ("regional_group", "Regional group"),
    ],
    "multi": [
        ("organisations", "Organisations"),
        ("country_code",  "Country code"),
    ],
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
    profile: dict = UPR_INFO_DEFAULTS,
) -> str:
    df = _prepare(df, profile)
    chart_list = charts_mod.auto_charts_from_df(
        df,
        categorical=profile.get("categorical", []),
        multi=profile.get("multi", []),
        has_year=True,
    )
    return render_dashboard(
        charts=chart_list,
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
                    help="Source instance URL (for footer display only)")
    args = ap.parse_args()

    if args.sample:
        # Lazy import so the runtime dep on tests/ isn't required for the
        # normal flow.
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from tests.fixtures.upr_info_sample import make_sample  # noqa: E402
        df = make_sample(200)
        instance = "https://upr-info-database.uwazi.io (sample fixture)"
    else:
        if not args.from_path.exists():
            ap.error(
                f"cache not found at {args.from_path}.\n"
                "Run `python -m uwazi_charts.fetch` first, or pass --sample."
            )
        df = load_cache(args.from_path)
        instance = args.instance

    html = build_html_from_df(df, instance_url=instance)
    write_dashboard(html, args.out)
    print(f"[build] {df.shape[0]:,} rows → {args.out}")


if __name__ == "__main__":
    main()
