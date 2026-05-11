# uwazi-statistics

Auto-generates a single static HTML dashboard with aggregate charts from
any [Uwazi](https://github.com/huridocs/uwazi) instance — no content,
no full-text search, no per-record drill-down. Just templates → fields
→ charts, customised to whatever metadata the instance has.

Schema is discovered from `/api/templates`; entities are paginated from
`/api/search`; everything is cached to local parquet so subsequent
builds are offline and fast.

---

## Requirements

- Python ≥ 3.11
- [uv](https://github.com/astral-sh/uv) (recommended) or vanilla `pip`
- ~1 GB free disk if you cache a large Uwazi instance

## Setup

```bash
git clone git@github.com:lszoszk/uwazi-statistics.git
cd uwazi-statistics
uv sync --extra dev          # creates .venv and installs everything
```

(or with vanilla pip: `python -m venv .venv && .venv/bin/pip install -e ".[dev]"`)

## Try it without touching any Uwazi instance

```bash
uv run python -m uwazi_charts.build --sample
open output/index.html       # macOS — or just double-click the file
```

This generates a complete dashboard from synthetic fixture data
(`tests/fixtures/upr_info_sample.py`). Useful for first-run validation
and for working offline / on a plane.

## Use against a real Uwazi instance

```bash
cp .env.example .env
# edit UWAZI_URL to point at the instance (default: UPR Info Database)
# customise UWAZI_USER_AGENT to identify yourself to the instance

# One-time fetch, cached to cache/entities.parquet
uv run python -m uwazi_charts.fetch --limit 5000        # debug: stop after N
uv run python -m uwazi_charts.fetch                     # full fetch (10-15 min for 264k)

# Build the dashboard from the cache (offline, fast)
uv run python -m uwazi_charts.build
open output/index.html
```

You can re-run `build` as many times as you want; only `fetch` hits the
network.

### Other instances

The script works against any Uwazi instance whose `/api/search` is
publicly readable. To point it elsewhere:

```bash
UWAZI_URL=https://your-instance.uwazi.io uv run python -m uwazi_charts.fetch
UWAZI_URL=https://your-instance.uwazi.io uv run python -m uwazi_charts.build
```

If the instance requires auth, set `UWAZI_USER` + `UWAZI_PASSWORD` in
`.env` (currently a TODO — the fetch module would need a login flow).

## What the dashboard looks like

- **Topbar** with brand mark, source URL, timestamp
- **KPI strip** — universal stats: Entities, Templates, Languages, Date range
- **Tabs** — one per template (most populous first), plus an "All"
  cross-template view
- **Numbered chart cards** — bar/line/doughnut, auto-picked from each
  template's schema. Multiselect fields are exploded; long-tail
  categorical fields get a Top-N + "Other" bucket
- **Footer** with attribution + methodology one-liner

## How discovery works

1. `fetch.fetch_templates()` → calls `/api/templates`, returns the schema
2. `discover.parse_templates()` → flattens templates × properties into
   `Field` records
3. `discover.build_profile()` → classifies properties by type:
   - `select`, `relationship` → categorical (bar chart)
   - `multiselect` → multi (bar chart, exploded)
   - `date`, `multidate` → date (line chart per year)
   - `text`, `markdown`, `numeric`, `geolocation`, `link`, `image`,
     `media`, `preview`, `daterange` → skipped (v1 doesn't chart these)
4. `discover.discover_profile_from_df()` → fallback when offline,
   sniffs metadata column shape directly. Also catches Uwazi's
   denormalised inherited fields like `state_under_review___regional_group`
   which only appear in entity metadata, never in template schemas
5. `discover.merge_profiles()` → schema-first (authoritative labels) +
   DF-inferred (catches inherited fields), deduped and trimmed

The default cap is 12 chart cards per tab to keep the page legible —
override via `discover.build_profile(fields, max_charts=N)`.

## Layout

```
uwazi_charts/
  fetch.py     paginated /api/search + /api/templates → parquet
  flatten.py   metadata dict → flat DataFrame columns
  discover.py  schema → chartable field profile (hybrid: schema + DF)
  charts.py    column → Chart.js-ready dict; compute_kpis()
  render.py    jinja2 wrapper
  build.py     CLI; orchestrates tab generation per template
templates/
  dashboard.html.j2
tests/
  fixtures/upr_info_sample.py    synthetic UPR-Info-shape DataFrame
  test_smoke.py                  end-to-end + KPI/tab structure
  test_discover.py               schema classification + DF inference
```

## Tests

```bash
uv run pytest -q
```

All 11 tests are offline and use the synthetic fixture; they shouldn't
hit any network.

## Style / linting

```bash
uv run ruff check .
uv run ruff format --check .
```

(Config in `pyproject.toml`.)

## Caching policy

- `cache/` and `output/` are gitignored — **never commit fetched data**
- Re-fetch only when you want a refresh; daily-monthly cadence is plenty
- Be polite to the source instance: the fetcher sleeps 100 ms between
  batches and sends a `User-Agent` you should personalise via `.env`

## License

(Add a LICENSE file if/when this leaves private — MIT or similar.)

## Notes

This is a v0.0.1 proof of concept. See `git log` for the recent
iteration history; see open TODOs in source for what's not done yet
(numeric histograms, geolocation maps, daterange Gantts, click-to-filter
between charts).
