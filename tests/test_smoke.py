"""End-to-end smoke test — synthetic fixture → HTML on disk.

Verifies the whole pipeline boots without needing network access.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tests.fixtures.upr_info_sample import make_sample
from uwazi_charts import charts as charts_mod
from uwazi_charts import flatten
from uwazi_charts.build import build_html_from_df


def test_fixture_shape():
    df = make_sample(50)
    assert len(df) == 50
    for col in ("_id", "title", "template", "language", "creationDate", "metadata"):
        assert col in df.columns
    sample_meta = df.iloc[0]["metadata"]
    assert "regional_group" in sample_meta
    assert sample_meta["regional_group"][0]["label"]


def test_flatten_round_trip():
    df = make_sample(20)
    df = flatten.add_label_column(df, "regional_group", multi=False)
    df = flatten.add_year_column(df)
    assert df["regional_group"].notna().any()
    assert df["year"].between(2015, 2025).all()


def test_add_session_year_column_extracts_year_from_label():
    """Real-world session labels look like '44 - November 2023'."""
    import pandas as pd
    df = pd.DataFrame([
        {"metadata": {"session": [{"label": "44 - November 2023"}]}},
        {"metadata": {"session": [{"label": "1 - April 2008"}]}},
        {"metadata": {"session": [{"label": "no year here"}]}},
        {"metadata": {"session": []}},
        {"metadata": None},
    ])
    out = flatten.add_session_year_column(df)
    # Use dropna() to get the parsed years; check NA-count separately.
    parsed = out["year"].dropna().astype(int).tolist()
    assert parsed == [2023, 2008]
    assert out["year"].isna().sum() == 3


def test_add_session_year_column_preserves_existing_when_overwrite_false():
    """Backstop: if a row has year already (from creationDate) AND no session,
    keep the original year."""
    import pandas as pd
    df = pd.DataFrame([
        {"metadata": {"session": [{"label": "44 - November 2023"}]}, "year": 1999},
        {"metadata": {"session": []},                                 "year": 2017},
    ])
    out = flatten.add_session_year_column(df, overwrite=False)
    assert out["year"].astype(int).tolist() == [2023, 2017]


def test_chart_has_labels_and_values():
    df = make_sample(100)
    df = flatten.add_label_column(df, "regional_group", multi=False)
    chart = charts_mod.bar_from_categorical(df, "regional_group", title="Regions")
    assert chart["kind"] == "bar"
    assert len(chart["labels"]) > 0
    assert sum(chart["values"]) <= 100


def test_stacked_bar_by_year_shape():
    df = make_sample(200)
    df = flatten.add_label_column(df, "regional_group", multi=False)
    df = flatten.add_year_column(df)
    chart = charts_mod.stacked_bar_by_year(df, "regional_group", title="Regions")

    assert chart["kind"] == "stacked_bar"
    # labels are years, ascending
    assert chart["labels"] == sorted(chart["labels"])
    assert all(isinstance(y, int) for y in chart["labels"])
    # datasets carry per-category series
    assert len(chart["datasets"]) >= 1
    for ds in chart["datasets"]:
        assert isinstance(ds["label"], str)
        assert len(ds["values"]) == len(chart["labels"])
    # year-totals across all series should equal the overall n
    total_from_pivot = sum(sum(ds["values"]) for ds in chart["datasets"])
    assert total_from_pivot == chart["n"]


def test_stacked_bar_empty_on_missing_column():
    df = make_sample(20)
    chart = charts_mod.stacked_bar_by_year(df, "nonexistent_field")
    assert chart["kind"] == "stacked_bar"
    assert chart["datasets"] == []
    assert charts_mod.chart_has_data(chart) is False


def test_auto_charts_inserts_stacked_companion():
    df = make_sample(150)
    df = flatten.add_label_column(df, "regional_group", multi=False)
    df = flatten.add_year_column(df)
    charts = charts_mod.auto_charts_from_df(
        df, categorical=[("regional_group", "Regional group")], has_year=True)

    kinds = [c["kind"] for c in charts]
    assert "stacked_bar" in kinds
    # The stacked variant should sit directly after its bar parent
    bar_idx = kinds.index("bar")
    assert kinds[bar_idx + 1] == "stacked_bar"


def test_auto_charts_skips_stacked_with_too_few_years():
    df = make_sample(30)
    df = flatten.add_label_column(df, "regional_group", multi=False)
    df["year"] = 2024  # collapse to one year
    charts = charts_mod.auto_charts_from_df(
        df, categorical=[("regional_group", "Regional group")], has_year=True,
        min_years_for_stack=3)
    assert all(c["kind"] != "stacked_bar" for c in charts)


def test_end_to_end_html(tmp_path: Path):
    df = make_sample(150)
    html = build_html_from_df(df, instance_url="https://example.org")
    assert "<canvas" in html
    assert "Regional Group" in html or "Regional group" in html or "Year" in html
    assert "150" in html  # total entities footer

    out = tmp_path / "index.html"
    out.write_text(html)
    assert out.exists()
    assert out.stat().st_size > 2000  # not a stub


def test_end_to_end_html_has_controls(tmp_path: Path):
    df = make_sample(150)
    html = build_html_from_df(df, instance_url="https://example.org")
    # percent toggle + CSV download wired into card markup
    assert 'data-scale="abs"' in html
    assert 'data-scale="pct"' in html
    assert 'data-csv=' in html
    # stacked-bar canvas wired into the JS payload
    assert '"stacked_bar"' in html


def test_kpi_strip_present(tmp_path: Path):
    df = make_sample(120)
    html = build_html_from_df(df, instance_url="https://example.org")
    # KPI strip + at least the two universal stats
    assert 'class="kpis"' in html
    assert "Entities" in html
    assert "120" in html
    # editorial branding cue (UHRI-style topbar)
    assert "UWAZI" in html
    assert "aggregate" in html
    # the chartable fields end up in numbered cards
    assert 'class="idx"' in html


def test_single_template_no_tab_strip():
    """Fixture has only one template → tab bar should be hidden."""
    df = make_sample(40)
    html = build_html_from_df(df, instance_url="https://example.org")
    # "All" pane still rendered, but the tabs nav is suppressed for 1 tab
    assert 'id="pane-all"' in html
    assert 'class="tabs"' not in html  # nav suppressed


def test_multi_template_renders_tab_strip():
    """Two templates + a schema → All + 2 per-template tabs."""
    df = make_sample(40).copy()
    # rewrite half the rows to a second template id
    df.loc[df.index[:20], "template"] = "tpl-state"
    df.loc[df.index[20:], "template"] = "tpl-pledge"
    templates = [
        {"_id": "tpl-state",  "name": "State",            "properties": []},
        {"_id": "tpl-pledge", "name": "Voluntary Pledge", "properties": []},
    ]
    html = build_html_from_df(df, instance_url="https://example.org",
                              schema_templates=templates)
    assert 'class="tabs"' in html
    assert 'pane-all' in html
    assert 'pane-state' in html
    assert 'pane-voluntary-pledge' in html
    # most-populous-first ordering: both have 20, so order is dict-insertion;
    # we just check both are present
    assert html.index('pane-state') < html.index('pane-voluntary-pledge') or \
           html.index('pane-voluntary-pledge') < html.index('pane-state')
