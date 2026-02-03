> [!CAUTION] Experimental

# west-discovery (STAC FastAPI)

A STAC API facade that uses Globus Search as the backend. This project adapts
`stac-fastapi` to query a Globus Search index and expose STAC Collections and
Items over HTTP, with optional aggregation and CQL2 JSON filtering.

## What This Provides

- A FastAPI-based STAC API server backed by Globus Search.
- STAC search with pagination tokens and optional CQL2 JSON filtering.
- Aggregation extension support (Globus Search aggregation client).
- Local collection schemas for `CMIP6` and `obs4MIPs`.
- Scripts to scrape STAC items from CEDA and ingest them into Globus Search.
- Docker and GitHub Actions workflow for build + ECS deployment.

## Repository Layout

- `src/stac_fastapi/globus_search/`: Globus Search backend implementation.
- `src/stac_fastapi/globus_search/schemas/`: Local STAC collection definitions.
- `scripts/`: Scrape, ingest, and local query helpers (using `pip-run`).
- `ecs/`: ECS task definition used by CI/CD.
- `Dockerfile`: Container build for the API.

## Requirements

- Python 3.12
- Globus Search access (read for API, write for ingest)

You can install dependencies via Poetry (`pyproject.toml`) or `requirements.txt`.
Note: the Poetry and `requirements.txt` dependency sets are not identical. Use
one approach consistently.

## Quickstart (Local API)

### 1) Install dependencies

With Poetry:

```bash
poetry install
```

With pip:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run the API

With Uvicorn directly:

```bash
uvicorn stac_fastapi.globus_search.app:handler --host 0.0.0.0 --port 8000 --reload
```

The API will serve STAC endpoints at `http://localhost:8000/`.

### 3) Cache directory

The API uses `hishel` with an on-disk SQLite cache at `cache/hishel_cache.db`.
Create the directory before first run if it doesn't exist:

```bash
mkdir -p cache
```

## Configuration

- **Search index**: set in `src/stac_fastapi/globus_search/config.py` as
  `SEARCH_INDEX_ID`. Update this to match the Globus Search index you intend
  to query.
- **Globus Search client**: created via `GlobusSearchSettings().create_client`.
  This relies on `globus-sdk` defaults and the `stac-fastapi` settings model.

If you need environment-driven configuration, add fields to
`GlobusSearchSettings` or refer to the upstream `stac-fastapi` `ApiSettings`
options.

## CQL2 JSON Filter Support

CQL2 JSON filters are translated into Globus Search filters. Supported operators
include:

- Boolean: `and`, `or`, `not`
- Comparison: `=`, `<>`, `<=`, `>=`, `in`, `isNull`
- Spatial: `s_intersects`, `s_within` (maps to geo_shape)

Unsupported operators raise `NotImplementedError` or `ValueError` (see
`cql_to_filter` in `src/stac_fastapi/globus_search/database_logic.py`).

## Docker

Build and run locally:

```bash
docker build -t west-discovery-api .
docker run --rm -p 8000:8000 west-discovery-api
```

The container runs Uvicorn with the ASGI cache middleware handler.

## CI/CD and Deployment

The GitHub Actions workflow (`.github/workflows/build-and-deploy.yml`):

- Builds and pushes a Docker image to ECR on pushes to `add-ci-cd`,
  `integration`, or `main`.
- Deploys to ECS integration on pushes to `add-ci-cd` or `integration`.
- Deploys to ECS production on merges to `main`.

ECS task definitions live in `ecs/task-definition.json`.

## Known Gaps

- No automated tests in `tests/` yet.
- Some CQL2 operators are intentionally unsupported.
- Search index ID is currently a code constant (not environment-driven).

## Contributing

- Keep changes aligned with `stac-fastapi` API expectations.
- Add tests when introducing new filter translations or request behaviors.
- Prefer updating `GlobusSearchSettings` for configuration rather than
  scattering environment reads.

## License

Apache-2.0
