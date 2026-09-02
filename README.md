# west-discovery

A STAC API backed by Globus Search. Exposes ESGF climate datasets as STAC Collections and Items over HTTP, with CQL2 JSON filtering, aggregation, free-text search, and queryables derived from ESGF controlled vocabularies.

## Repository Layout

| Path | Purpose |
|---|---|
| `src/stac_fastapi/globus_search/` | Core backend implementation |
| `Dockerfile` | Multi-stage container build |

## Local Development

```bash
# Build the development image (no SSL, hot reload)
docker build --target development -t discovery-api:dev .

# Run with source mounted for hot reload
docker run -p 8000:8000 -v ./src/stac_fastapi:/var/task/stac_fastapi discovery-api:dev
```

The API is available at `http://localhost:8000`.

## Running Without Docker

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
esgvoc use cmip6@latest \
    && esgvoc use cmip6plus@latest \
    && esgvoc use cordex-cmip6@latest \
    && esgvoc use cmip7@latest \
    && esgvoc use obs4ref@latest \
    && esgvoc use universe@latest
uvicorn stac_fastapi.globus_search.app:handler --host 0.0.0.0 --port 8000 --reload
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest
# With coverage
coverage run -m pytest && coverage report
```

## Configuration

`SEARCH_INDEX_ID` in `src/stac_fastapi/globus_search/config.py` controls which Globus Search index is queried. Authentication uses `globus-sdk` defaults.

## Collections

Collections are derived from ESGF controlled vocabularies via [esgvoc](https://github.com/ESGF/esgvoc). The following projects are supported:

| Collection | Notes |
|---|---|
| CMIP6 | |
| CMIP6Plus | |
| CMIP7 | |
| CORDEX-CMIP6 | |
| obs4REF | |
| CMIP6Test | Integration environment only |

## Extensions

| Extension | Notes |
|---|---|
| Filter (CQL2 JSON) | `and`, `or`, `not`, `=`, `<>`, `<=`, `>=`, `in`, `isNull`, `like`, `s_intersects`, `s_within` |
| Aggregation | Backed by Globus Search aggregations |
| Free-text | OR-joined query string |
| Token pagination | Globus Search scroll marker |
| Queryables | Derived from esgvoc controlled vocabularies; falls back to item sampling for unknown collections |

## CI/CD

Deployment is managed via Github Actions in a separate private repository. The production Docker target is used for all CI builds.

## License

Apache-2.0
