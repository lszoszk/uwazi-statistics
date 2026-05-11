"""Tests for the schema-discovery module."""

from __future__ import annotations

from tests.fixtures.upr_info_sample import make_sample
from uwazi_charts import discover


SAMPLE_TEMPLATES = [
    {
        "_id": "tpl_recommendation",
        "name": "Recommendation",
        "properties": [
            {"name": "regional_group", "label": "Regional group", "type": "select"},
            {"name": "organisations",  "label": "Organisations",  "type": "multiselect"},
            {"name": "country_code",   "label": "Country code",   "type": "text"},
            {"name": "issue_date",     "label": "Issue date",     "type": "date"},
            {"name": "narrative",      "label": "Narrative",      "type": "markdown"},
            {"name": "score",          "label": "Score",          "type": "numeric"},
            {"name": "linked",         "label": "Linked",         "type": "relationship"},
        ],
    },
    {
        "_id": "tpl_pledge",
        "name": "Voluntary pledge",
        "properties": [
            {"name": "regional_group", "label": "Regional group", "type": "select"},  # duplicate name
            {"name": "pledged_at",     "label": "Pledged at",     "type": "date"},
        ],
    },
]


def test_parse_templates_skips_system_and_invalid():
    fields = discover.parse_templates(SAMPLE_TEMPLATES)
    names = [f.name for f in fields]
    assert "regional_group" in names
    assert "narrative" in names           # parser keeps; profile-builder will drop
    assert "creationDate" not in names    # system property excluded


def test_build_profile_classifies_types():
    fields = discover.parse_templates(SAMPLE_TEMPLATES)
    profile = discover.build_profile(fields)

    cat_names = {n for n, _ in profile["categorical"]}
    multi_names = {n for n, _ in profile["multi"]}
    date_names = {n for n, _ in profile["date"]}

    assert "regional_group" in cat_names   # select
    assert "linked" in cat_names           # relationship
    assert "organisations" in multi_names  # multiselect
    assert "issue_date" in date_names      # date
    assert "pledged_at" in date_names

    # types we explicitly don't chart in v1
    assert "narrative" not in cat_names | multi_names | date_names
    assert "country_code" not in cat_names | multi_names | date_names  # text
    assert "score" not in cat_names | multi_names | date_names         # numeric


def test_build_profile_dedupes_across_templates():
    fields = discover.parse_templates(SAMPLE_TEMPLATES)
    profile = discover.build_profile(fields)
    cat_names = [n for n, _ in profile["categorical"]]
    assert cat_names.count("regional_group") == 1


def test_discover_from_df_finds_metadata_fields():
    df = make_sample(50)
    profile = discover.discover_profile_from_df(df)
    cat = {n for n, _ in profile["categorical"]}
    multi = {n for n, _ in profile["multi"]}
    # fixture always has 1 region per row → categorical
    assert "regional_group" in cat
    # fixture has 1-3 orgs per row → multi (eventually a row has >1)
    assert "organisations" in multi
