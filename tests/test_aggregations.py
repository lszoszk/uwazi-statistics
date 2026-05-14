"""Tests for the aggregations adapter + RISON parser.

These tests use canned payloads — no network. The shape matches what the
live UPR Info instance returned during the 2026-05 audit; if the live API
shape changes, regenerate the fixtures with:

    curl -sG "https://upr-info-database.uwazi.io/api/search" \\
      --data-urlencode 'limit=0' \\
      --data-urlencode 'types[]=5d8ce04361cde0408222e9a8' \\
        | python -m json.tool > tests/fixtures/upr_aggs.json
"""

from __future__ import annotations

import pytest

from uwazi_charts import aggregations as agg
from uwazi_charts import rison


# ─────────────────────────────────────────────────────────────────────────────
# RISON parser — Library URL state
# ─────────────────────────────────────────────────────────────────────────────

def test_rison_simple_object():
    out = rison.loads("(a:1,b:2)")
    assert out == {"a": 1, "b": 2}


def test_rison_booleans_and_null():
    assert rison.loads("(t:!t,f:!f,n:!n)") == {"t": True, "f": False, "n": None}


def test_rison_strings_with_escapes():
    # !' escapes a single quote inside a string; !! escapes a !
    assert rison.loads("'O!'Reilly'") == "O'Reilly"
    assert rison.loads("'foo!!bar'") == "foo!bar"


def test_rison_arrays_and_identifiers():
    assert rison.loads("!(a,b,c)") == ["a", "b", "c"]


def test_rison_nested_uwazi_shape():
    """The real Library URL state — mix of objects, arrays, booleans, strings."""
    s = (
        "(includeUnpublished:!t,order:desc,sort:creationDate,"
        "types:!('5d8ce04361cde0408222e9a8'),"
        "filters:(response:(values:!('34b4d35c-8157-40cf-a42f-c3cd7353d692'))))"
    )
    out = rison.loads(s)
    assert out["includeUnpublished"] is True
    assert out["order"] == "desc"
    assert out["types"] == ["5d8ce04361cde0408222e9a8"]
    assert out["filters"]["response"]["values"] == [
        "34b4d35c-8157-40cf-a42f-c3cd7353d692"
    ]


def test_rison_malformed_raises():
    with pytest.raises(rison.RisonError):
        rison.loads("(a:1,b:")


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation adapter
# ─────────────────────────────────────────────────────────────────────────────

CANNED_PAYLOAD = {
    "totalRows": 10000,
    "relation": "gte",
    "aggregations": {"all": {
        "action_category": {
            "doc_count": 117289,
            "buckets": [
                {"key": "k1", "doc_count": 103474,
                 "filtered": {"doc_count": 45949}, "label": "4 - General action"},
                {"key": "k2", "doc_count": 90262,
                 "filtered": {"doc_count": 22132}, "label": "5 - Specific action"},
                {"key": "k3", "doc_count": 48742,
                 "filtered": {"doc_count": 0}, "label": "2 - Continuing action"},
            ],
        },
        "issues": {
            "doc_count": 53820,
            "buckets": [
                {"key": "i1", "doc_count": 53820,
                 "filtered": {"doc_count": 23788}, "label": "Women's rights"},
                {"key": "i2", "doc_count": 41210,
                 "filtered": {"doc_count": 10210}, "label": "Rights of the Child"},
            ],
        },
        "session": {
            "doc_count": 117289,
            "buckets": [
                {"key": "s1", "doc_count": 1000,
                 "filtered": {"doc_count": 1000}, "label": "44 - November 2023"},
                {"key": "s2", "doc_count": 800,
                 "filtered": {"doc_count": 800}, "label": "43 - May 2023"},
                {"key": "s3", "doc_count": 500,
                 "filtered": {"doc_count": 500}, "label": "1 - April 2008"},
                {"key": "s4", "doc_count": 300,
                 "filtered": {"doc_count": 300}, "label": "Unknown session"},
            ],
        },
        "related_document": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {"key": "doc-abc", "doc_count": 12, "filtered": {"doc_count": 12}},
            ],
        },
    }},
}


def test_aggregations_to_charts_bar_uses_filtered_count():
    plan = [{"field": "action_category", "title": "Action", "kind": "bar"}]
    charts = agg.aggregations_to_charts(CANNED_PAYLOAD, plan)
    assert len(charts) == 1
    c = charts[0]
    assert c["kind"] == "bar"
    # zero-filtered bucket should disappear; remaining sorted desc
    assert c["labels"] == ["4 - General action", "5 - Specific action"]
    assert c["values"] == [45949, 22132]
    assert c["n"] == 45949 + 22132


def test_aggregations_to_charts_session_line_year_extraction():
    plan = [{"field": "session", "title": "Per year", "kind": "line_year"}]
    charts = agg.aggregations_to_charts(CANNED_PAYLOAD, plan)
    c = charts[0]
    assert c["kind"] == "line"
    # 2008 (500) + 2023 (1000 + 800 = 1800); session without year is skipped
    assert c["labels"] == [2008, 2023]
    assert c["values"] == [500, 1800]


def test_aggregations_to_charts_label_map_fallback():
    plan = [{"field": "related_document", "title": "Docs", "kind": "bar"}]
    charts = agg.aggregations_to_charts(
        CANNED_PAYLOAD, plan, label_map={"doc-abc": "Annual report 2024"})
    assert charts[0]["labels"] == ["Annual report 2024"]


def test_aggregations_to_charts_skips_missing_fields():
    plan = [
        {"field": "action_category", "title": "Action", "kind": "bar"},
        {"field": "nonexistent",     "title": "?",      "kind": "bar"},
    ]
    charts = agg.aggregations_to_charts(CANNED_PAYLOAD, plan)
    assert len(charts) == 1
    assert charts[0]["id"] == "action_category"


# ─────────────────────────────────────────────────────────────────────────────
# Chart plan
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_TEMPLATES = [
    {"_id": "tpl-rec", "name": "Recommendation", "properties": [
        {"name": "action_category", "label": "Action Category", "type": "select"},
        {"name": "response",        "label": "Response",        "type": "select"},
        {"name": "issues",          "label": "Issues",          "type": "multiselect"},
        {"name": "session",         "label": "Session",         "type": "relationship"},
        {"name": "recommendation",  "label": "Text",            "type": "markdown"},
    ]},
]


def test_default_chart_plan_excludes_markdown_and_includes_session_year():
    plan = agg.default_chart_plan(SCHEMA_TEMPLATES, type_ids=["tpl-rec"])
    fields = [p["field"] for p in plan]
    kinds = [p["kind"] for p in plan]
    assert "recommendation" not in fields                # markdown → out
    assert {"action_category", "response", "issues", "session"} <= set(fields)
    # session gets BOTH a bar (relationship) AND a line_year companion
    assert "line_year" in kinds


def test_default_chart_plan_respects_type_filter():
    extra = {"_id": "tpl-other", "name": "Other", "properties": [
        {"name": "foo", "label": "Foo", "type": "select"}]}
    plan = agg.default_chart_plan(
        SCHEMA_TEMPLATES + [extra], type_ids=["tpl-rec"])
    assert "foo" not in [p["field"] for p in plan]


# ─────────────────────────────────────────────────────────────────────────────
# build_embed_html — smoke
# ─────────────────────────────────────────────────────────────────────────────

def test_build_embed_html_emits_self_contained_page():
    from uwazi_charts.build import build_embed_html
    html = build_embed_html(
        instance_url="https://upr-info-database.uwazi.io",
        schema_templates=SCHEMA_TEMPLATES,
        type_ids=["tpl-rec"],
    )
    # Core wiring landed
    assert "Chart.js" not in html  # we use Chart, not the literal string
    assert "chart.js" in html.lower()
    # Config baked in — instance_url + types + chart_plan visible
    assert "upr-info-database.uwazi.io" in html
    assert "tpl-rec" in html
    assert "action_category" in html and "issues" in html
    # JS shipped — RISON parser, deck download, fetch loop
    assert "risonLoads" in html
    assert "btn-download-all" in html
    assert "/api/search" in html

