# uwazi-charts

Auto-generate static aggregate dashboards from any Uwazi instance.

The script discovers the instance's schema via `/api/templates`, fetches
all entities via `/api/search` (cached to local parquet), and renders a
single static HTML with charts for each chart-worthy property
(select / multiselect / date / numeric / geolocation).

No content browsing, no full-text search, no individual records — just
aggregates. Designed to be one Python file, one HTML file, no framework.

## Quick start (offline / sample)

```bash
uv sync                                # or: pip install -e .[dev]
python -m uwazi_charts.build --sample  # builds output/index.html from synthetic fixture
open output/index.html
```

## Quick start (live instance)

```bash
cp .env.example .env
# edit UWAZI_URL

# One-time fetch — cached to cache/entities.parquet
python -m uwazi_charts.fetch

# Build the dashboard from the cache (offline, fast)
python -m uwazi_charts.build
open output/index.html
```

## Layout

```
uwazi_charts/
  fetch.py     # paginated fetch → cache/entities.parquet
  flatten.py   # metadata → flat columns
  charts.py    # column → Chart.js-ready data
  render.py    # jinja2 → HTML
  build.py     # CLI entry
templates/
  dashboard.html.j2
tests/fixtures/
  upr_info_sample.py
```

## Status

Day-1 skeleton. See TODO in each module for the next planned step.
