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


def test_chart_has_labels_and_values():
    df = make_sample(100)
    df = flatten.add_label_column(df, "regional_group", multi=False)
    chart = charts_mod.bar_from_categorical(df, "regional_group", title="Regions")
    assert chart["kind"] == "bar"
    assert len(chart["labels"]) > 0
    assert sum(chart["values"]) <= 100


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
