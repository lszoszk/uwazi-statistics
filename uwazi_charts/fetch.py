"""Paginated fetch from an Uwazi instance, cached to parquet.

Uwazi exposes `/api/search` without auth on public instances (e.g. UPR Info
Database). Each call returns up to `limit` entities, offset by `from`. We
loop until an empty batch comes back, save to a local parquet, and return
a DataFrame.

CLI:
    python -m uwazi_charts.fetch                       # uses UWAZI_URL from .env
    python -m uwazi_charts.fetch --instance https://...
    python -m uwazi_charts.fetch --limit 1000          # stop after first N (debug)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

DEFAULT_BATCH = 500
DEFAULT_LANGUAGE = "en"
DEFAULT_TIMEOUT_S = 60
MAX_RETRIES = 4
RETRY_BACKOFF_S = 2.0


@dataclass
class FetchConfig:
    instance_url: str
    language: str = DEFAULT_LANGUAGE
    batch_size: int = DEFAULT_BATCH
    user_agent: str = "uwazi-charts-poc"
    max_records: int | None = None  # for debug — stop early


def _make_session(user_agent: str, language: str = DEFAULT_LANGUAGE) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/json",
        # Uwazi rejects `?language=` on /api/search; use Accept-Language instead.
        "Accept-Language": language,
    })
    return s


def _get_with_retry(session: requests.Session, url: str, **kwargs) -> requests.Response:
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=DEFAULT_TIMEOUT_S, **kwargs)
            if r.status_code < 500:
                r.raise_for_status()
                return r
            last_err = RuntimeError(f"{r.status_code} {r.text[:200]}")
        except (requests.ConnectionError, requests.Timeout) as e:
            last_err = e
        sleep_for = RETRY_BACKOFF_S * (2 ** attempt)
        time.sleep(sleep_for)
    raise RuntimeError(f"fetch failed after {MAX_RETRIES} retries: {last_err}")


def fetch_templates(config: FetchConfig) -> list[dict]:
    """Return the list of templates (entity schemas) on this instance."""
    session = _make_session(config.user_agent, config.language)
    r = _get_with_retry(session, f"{config.instance_url}/api/templates")
    payload = r.json()
    # Uwazi returns {"rows": [...]}
    return payload.get("rows", payload) if isinstance(payload, dict) else payload


def fetch_entities(config: FetchConfig) -> pd.DataFrame:
    """Paginate through /api/search until empty. Returns one DataFrame."""
    session = _make_session(config.user_agent, config.language)
    rows: list[dict] = []
    offset = 0

    # First probe to learn the total (Elasticsearch caps precise count at 10k
    # by default; for bigger sets `totalRows` will read 10000 with
    # relation=gte — we trust the loop-until-empty exit anyway).
    probe = _get_with_retry(
        session,
        f"{config.instance_url}/api/search",
        params={"limit": 1},
    ).json()
    total = probe.get("totalRows")
    pbar = tqdm(total=total if isinstance(total, int) else None, unit="rec", desc="fetch")

    while True:
        params = {
            "limit": config.batch_size,
            "from": offset,
        }
        r = _get_with_retry(session, f"{config.instance_url}/api/search", params=params)
        batch = r.json().get("rows", [])
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        pbar.update(len(batch))

        if config.max_records and len(rows) >= config.max_records:
            rows = rows[: config.max_records]
            break
        # Defensive: if the API returned fewer than asked, we're done.
        if len(batch) < config.batch_size:
            break
        # Courteous pause to spare the instance.
        time.sleep(0.1)

    pbar.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def save_cache(df: pd.DataFrame, path: Path) -> None:
    """Persist DataFrame to parquet. Metadata column is a dict — we serialise
    it as JSON string so parquet can store it cleanly across versions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    if "metadata" in df.columns:
        df["metadata"] = df["metadata"].apply(json.dumps)
    df.to_parquet(path, compression="zstd")


def load_cache(path: Path) -> pd.DataFrame:
    """Inverse of save_cache — reload + parse metadata back to dicts."""
    df = pd.read_parquet(path)
    if "metadata" in df.columns:
        df["metadata"] = df["metadata"].apply(lambda s: json.loads(s) if isinstance(s, str) else s)
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--instance", default=os.environ.get("UWAZI_URL"),
                    help="Uwazi instance base URL (or set UWAZI_URL env)")
    ap.add_argument("--language", default=DEFAULT_LANGUAGE)
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after fetching N records (debug)")
    ap.add_argument("--out", type=Path, default=Path("cache/entities.parquet"))
    ap.add_argument("--user-agent",
                    default=os.environ.get("UWAZI_USER_AGENT", "uwazi-charts-poc"))
    args = ap.parse_args()

    if not args.instance:
        ap.error("missing --instance (or UWAZI_URL in .env)")

    config = FetchConfig(
        instance_url=args.instance.rstrip("/"),
        language=args.language,
        batch_size=args.batch_size,
        user_agent=args.user_agent,
        max_records=args.limit,
    )

    print(f"[fetch] discovering templates at {config.instance_url} …")
    templates = fetch_templates(config)
    print(f"[fetch] found {len(templates)} templates")

    print(f"[fetch] pulling entities (batch={config.batch_size}, "
          f"limit={config.max_records or 'all'}) …")
    df = fetch_entities(config)
    print(f"[fetch] got {len(df):,} entities")

    save_cache(df, args.out)
    print(f"[fetch] saved → {args.out} ({args.out.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
