"""Synthetic fixture mirroring the UPR Info Database entity shape.

Use for offline dev + unit tests. The structure matches what
`/api/search` returns on the real instance — see fetch.py for the
real-world shape.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


_REGIONS = [
    ("61bd3790-b038-4819-8b96-4a76da132006", "Africa (African Group)"),
    ("e3a4a718-bc47-4f4f-a00c-60a40d68598a", "Asia-Pacific"),
    ("0d7a338c-7bbc-411e-934f-47bca9f811b4", "Western Europe & Others"),
    ("3d7eb25c-ff23-4835-bbdf-6bebdd115e63", "Eastern Europe"),
    ("f4d9ab3a-71d4-412d-b048-e0389609b937", "Latin America & Caribbean"),
]

_ORGS = [
    ("a803d223-1905-4ac0-8c43-7286f967a036", "AU (African Union)"),
    ("d1fdda67-7755-43aa-ae51-1cdd1d3a7049", "OIF (Organisation internationale de la Francophonie)"),
    ("b1c0e7a9-5a4e-4f25-9c81-aa9d3f3a9c11", "EU (European Union)"),
    ("c3a5d2b1-8e6f-4a73-bc12-0fed8c2b56e2", "OAS (Organisation of American States)"),
    ("d6f4a8e9-1c3b-4d7a-9f02-a1b2c3d4e5f6", "ASEAN"),
]

_COUNTRIES = ["BI", "FR", "DE", "BR", "JP", "KE", "MX", "CA", "AU", "PL", "IN", "ZA", "AR", "EG"]


def make_sample(n: int = 200, seed: int = 42) -> pd.DataFrame:
    """Return `n` synthetic entities mimicking UPR Info's API response shape.

    Shape (per row):
        _id, title, template, language, creationDate, sharedId, published, metadata
    where metadata is the nested dict that flatten.py expects.
    """
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        # Each entity gets 1 region + 1-3 organisations + 1 country.
        region = _REGIONS[int(rng.integers(0, len(_REGIONS)))]
        n_orgs = int(rng.integers(1, 4))
        orgs = [_ORGS[k] for k in rng.choice(len(_ORGS), size=n_orgs, replace=False)]
        country = _COUNTRIES[int(rng.integers(0, len(_COUNTRIES)))]
        creation_year = int(rng.integers(2015, 2026))
        creation_ms = int(pd.Timestamp(f"{creation_year}-06-15", tz="UTC").timestamp() * 1000)

        rows.append({
            "_id": f"fake_{i:05d}",
            "title": f"Sample entity {i}",
            "template": "fixture-template",
            "language": "en",
            "published": True,
            "sharedId": f"sid_{i:05d}",
            "creationDate": creation_ms,
            "editDate": creation_ms,
            "metadata": {
                "regional_group": [{"value": region[0], "label": region[1]}],
                "organisations":  [{"value": o[0], "label": o[1]} for o in orgs],
                "country_code":   [{"value": country}],
            },
        })
    return pd.DataFrame(rows)
