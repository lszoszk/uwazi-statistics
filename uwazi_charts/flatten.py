"""Turn Uwazi's nested `metadata` dict into flat DataFrame columns.

Uwazi entity shape (simplified):

    {
        "_id": "...",
        "title": "...",
        "template": "<template-id>",
        "language": "en",
        "creationDate": 1569513208984,   # ms epoch
        "metadata": {
            "regional_group": [{"value": "<uuid>", "label": "Africa"}],
            "organisations":  [{"value": "...",    "label": "AU"}, ...],
            "country_code":   [{"value": "BI"}],     # text-like (no label)
            "issue_date":     [{"value": 1569513200}],
        },
    }

`select` / `multiselect` / `relationship` properties yield lists of
`{value, label}`. `text` / `numeric` / `date` yield lists with just
`value`. Empty fields can be missing entirely OR present as `[]`.
"""

from __future__ import annotations

import pandas as pd

# ---------- single-row extractors ----------

def extract_labels(metadata: dict | None, key: str) -> list[str]:
    """For select/multiselect/relationship: return [label, label, ...] or []."""
    if not metadata:
        return []
    items = metadata.get(key) or []
    return [it.get("label") for it in items if isinstance(it, dict) and it.get("label")]


def extract_values(metadata: dict | None, key: str) -> list:
    """For text/numeric/date: return [value, value, ...] or []."""
    if not metadata:
        return []
    items = metadata.get(key) or []
    return [it.get("value") for it in items if isinstance(it, dict) and "value" in it]


def extract_first_label(metadata: dict | None, key: str) -> str | None:
    labels = extract_labels(metadata, key)
    return labels[0] if labels else None


def extract_first_value(metadata: dict | None, key: str):
    values = extract_values(metadata, key)
    return values[0] if values else None


# ---------- DataFrame-level helpers ----------

def add_label_column(df: pd.DataFrame, key: str, *, multi: bool = False) -> pd.DataFrame:
    """Add `df[key]` as either a scalar (multi=False, first label) or a list
    (multi=True, all labels). Source is `df['metadata']`."""
    if "metadata" not in df.columns:
        raise KeyError("DataFrame has no 'metadata' column — load via fetch.load_cache()?")
    df = df.copy()
    if multi:
        df[key] = df["metadata"].apply(lambda m: extract_labels(m, key))
    else:
        df[key] = df["metadata"].apply(lambda m: extract_first_label(m, key))
    return df


def add_value_column(df: pd.DataFrame, key: str, *, multi: bool = False) -> pd.DataFrame:
    """Same as add_label_column but for text/numeric/date properties."""
    if "metadata" not in df.columns:
        raise KeyError("DataFrame has no 'metadata' column")
    df = df.copy()
    if multi:
        df[key] = df["metadata"].apply(lambda m: extract_values(m, key))
    else:
        df[key] = df["metadata"].apply(lambda m: extract_first_value(m, key))
    return df


def add_year_column(df: pd.DataFrame, src: str = "creationDate", dst: str = "year") -> pd.DataFrame:
    """Convert ms-epoch column to year (UTC)."""
    df = df.copy()
    df[dst] = pd.to_datetime(df[src], unit="ms", utc=True, errors="coerce").dt.year
    return df


import re as _re

_SESSION_YEAR_RX = _re.compile(r"\b(19|20)\d{2}\b")


def _extract_session_year(metadata: dict | None, session_key: str = "session") -> int | None:
    """Pull the year out of a Session relationship's label.

    Sessions on the UPR Info instance carry labels like `"44 - November 2023"`.
    We grab the first 4-digit year out of that label. Returns None if no
    session, no label, or no year-shaped substring.
    """
    if not isinstance(metadata, dict):
        return None
    items = metadata.get(session_key) or []
    for it in items if isinstance(items, list) else [items]:
        if not isinstance(it, dict):
            continue
        label = it.get("label") or ""
        m = _SESSION_YEAR_RX.search(label)
        if m:
            return int(m.group(0))
    return None


def add_session_year_column(
    df: pd.DataFrame,
    *,
    src: str = "session",
    dst: str = "year",
    overwrite: bool = False,
) -> pd.DataFrame:
    """Add a `year` column derived from the Session relationship label.

    Cycle/session timing is the *meaningful* time axis for UPR data —
    `creationDate` records when the entity was added to Uwazi, which can lag
    the underlying review by years. Use this in preference to
    `add_year_column` whenever the data carries a `session` relationship.

    `overwrite=False` (default) preserves an existing `year` column for any
    row where session-year parsing fails — useful when chaining with
    `add_year_column(src="creationDate")` as a backstop.
    """
    if "metadata" not in df.columns:
        raise KeyError("DataFrame has no 'metadata' column")
    df = df.copy()
    sess_year = df["metadata"].apply(lambda m: _extract_session_year(m, src))
    if dst in df.columns and not overwrite:
        # Fill only the rows where session-year parsed successfully.
        df[dst] = sess_year.where(sess_year.notna(), df[dst])
    else:
        df[dst] = sess_year
    # Cast to nullable Int (preserves NaN cleanly across pandas versions).
    df[dst] = df[dst].astype("Int64")
    return df
