"""Turn DataFrame columns into Chart.js-ready dicts.

Each function returns:

    {
        "id":    "<safe-id>",
        "title": "<human title>",
        "kind":  "bar" | "line" | "doughnut",
        "labels": [...],
        "values": [...],
        "n":     <total entities counted>,
    }

The template loops over a list of these dicts and renders one <canvas>
per chart, hydrated by a single inline <script>.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd


def _safe_id(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s).strip("_").lower() or "chart"


def bar_from_categorical(
    df: pd.DataFrame,
    column: str,
    *,
    title: str | None = None,
    top_n: int = 15,
    multi: bool = False,
    other_bucket: bool = True,
) -> dict:
    """Build a horizontal-bar chart of the top-N most common values in
    `df[column]`.

    multi=True   — `df[column]` is a list-of-strings (multiselect); explode first
    multi=False  — `df[column]` is a scalar
    other_bucket — if True, lump everything beyond top_n into "Other"
    """
    series = df[column]
    if multi:
        series = series.explode()
    series = series.dropna()
    if series.empty:
        return _empty_chart(_safe_id(column), title or column, "bar")

    counts = series.value_counts()
    if len(counts) > top_n and other_bucket:
        head = counts.head(top_n)
        other = counts.iloc[top_n:].sum()
        labels = [*head.index.astype(str).tolist(), "Other"]
        values = [*head.values.tolist(), int(other)]
    else:
        head = counts.head(top_n)
        labels = head.index.astype(str).tolist()
        values = [int(v) for v in head.values.tolist()]

    return {
        "id": _safe_id(column),
        "title": title or column.replace("_", " ").title(),
        "kind": "bar",
        "labels": labels,
        "values": values,
        "n": int(series.shape[0]),
    }


def line_from_years(
    df: pd.DataFrame,
    column: str = "year",
    *,
    title: str | None = None,
) -> dict:
    """Build a line chart of entity count per year. Expects `df[column]` to
    already be integer years (use flatten.add_year_column)."""
    series = df[column].dropna().astype(int)
    if series.empty:
        return _empty_chart(_safe_id(column), title or column, "line")

    counts = series.value_counts().sort_index()
    return {
        "id": _safe_id(column),
        "title": title or "Entities per year",
        "kind": "line",
        "labels": [int(y) for y in counts.index.tolist()],
        "values": [int(v) for v in counts.values.tolist()],
        "n": int(series.shape[0]),
    }


def doughnut_from_categorical(
    df: pd.DataFrame, column: str, *, title: str | None = None, top_n: int = 6
) -> dict:
    """Doughnut chart — only worth using on low-cardinality fields
    (e.g. language, published, top-5 templates)."""
    c = bar_from_categorical(df, column, title=title, top_n=top_n, multi=False)
    c["kind"] = "doughnut"
    return c


# ---------- batch helpers ----------

def auto_charts_from_df(
    df: pd.DataFrame,
    *,
    categorical: Iterable[tuple[str, str]] = (),   # [(column, "Human Title"), ...]
    multi: Iterable[tuple[str, str]] = (),         # multiselect columns
    has_year: bool = True,
) -> list[dict]:
    """Convenience: turn a list of (column, title) hints into a chart list.

    For v1 we keep this explicit — the caller decides which fields to chart.
    `build.py` will eventually call `templates.discover_chartable_fields()`
    to populate this automatically from the Uwazi schema."""
    charts: list[dict] = []
    if has_year and "year" in df.columns:
        charts.append(line_from_years(df))
    for col, title in categorical:
        if col in df.columns:
            charts.append(bar_from_categorical(df, col, title=title, multi=False))
    for col, title in multi:
        if col in df.columns:
            charts.append(bar_from_categorical(df, col, title=title, multi=True))
    return charts


def compute_kpis(df: pd.DataFrame) -> list[dict]:
    """Return the top-of-page KPI strip — universal stats that work for any
    Uwazi instance.

    Each KPI dict: {"label": str, "value": str, "sub": str | None}
    where sub is an optional second-line caption (date range, etc.).
    """
    if df.empty:
        return [{"label": "Entities", "value": "0", "sub": "no data"}]

    kpis: list[dict] = []
    kpis.append({
        "label": "Entities",
        "value": f"{len(df):,}",
        "sub": None,
    })

    if "template" in df.columns:
        kpis.append({
            "label": "Templates",
            "value": f"{df['template'].nunique():,}",
            "sub": None,
        })

    if "language" in df.columns and df["language"].notna().any():
        langs = df["language"].dropna().unique()
        kpis.append({
            "label": "Languages",
            "value": str(len(langs)),
            "sub": ", ".join(sorted(langs[:4])) + ("…" if len(langs) > 4 else ""),
        })

    if "year" in df.columns and df["year"].notna().any():
        years = df["year"].dropna().astype(int)
        kpis.append({
            "label": "Date range",
            "value": f"{years.min()}–{years.max()}",
            "sub": f"{years.max() - years.min() + 1} years",
        })

    return kpis


def _empty_chart(cid: str, title: str, kind: str) -> dict:
    return {"id": cid, "title": title, "kind": kind, "labels": [], "values": [], "n": 0}
