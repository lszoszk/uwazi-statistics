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
