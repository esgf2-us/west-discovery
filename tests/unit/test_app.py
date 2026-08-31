import asyncio
import builtins
import sys
from types import SimpleNamespace

import pytest
from hishel.asgi import ASGICacheMiddleware
from stac_fastapi.api.app import StacApi
from starlette.exceptions import HTTPException
from stac_fastapi.extensions.core import (
    AggregationExtension,
    FilterExtension,
    FreeTextExtension,
    TokenPaginationExtension,
)
from stac_fastapi.extensions.core.free_text import FreeTextConformanceClasses

from stac_fastapi.globus_search import app
from stac_fastapi.globus_search.core import GlobusSearchClient
from stac_fastapi.globus_search.database_logic import DatabaseLogic
from stac_fastapi.globus_search.extensions.aggregration import (
    GlobusAggregationExtensionGetRequest,
    GlobusAggregationExtensionPostRequest,
)
from stac_fastapi.globus_search.extensions.aggregration.client import (
    GlobusSearchAggregationClient,
)


def test_app_wires_database_session_client_and_handler():
    assert isinstance(app.database_logic, DatabaseLogic)
    assert isinstance(app.api, StacApi)
    assert isinstance(app.api.client, GlobusSearchClient)
    assert app.api.client.database is app.database_logic
    assert app.api.client.session is app.session
    assert isinstance(app.handler, ASGICacheMiddleware)
    assert app.handler.app is app.api.app


def test_app_wires_extensions():
    assert app.extensions == [
        app.aggregation_extension,
        app.filter_extension,
        app.free_text_extension,
        app.pagination_extension,
    ]
    assert app.search_extensions == [
        app.filter_extension,
        app.free_text_extension,
        app.pagination_extension,
    ]
    assert isinstance(app.aggregation_extension, AggregationExtension)
    assert isinstance(app.filter_extension, FilterExtension)
    assert isinstance(app.free_text_extension, FreeTextExtension)
    assert isinstance(app.pagination_extension, TokenPaginationExtension)
    assert isinstance(app.aggregation_extension.client, GlobusSearchAggregationClient)
    assert app.aggregation_extension.client.database is app.database_logic


def test_app_customizes_aggregation_and_filter_extensions():
    assert app.aggregation_extension.GET is GlobusAggregationExtensionGetRequest
    assert app.aggregation_extension.POST is GlobusAggregationExtensionPostRequest
    assert (
        "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
        in app.filter_extension.conformance_classes
    )
    assert app.free_text_extension.conformance_classes == [
        FreeTextConformanceClasses.SEARCH
    ]


def test_app_configures_cached_collection_items_route():
    paths, dependencies = app.route_dependencies[0]

    assert paths == [{"path": "/collections/{collection_id}/items", "method": "GET"}]
    assert len(dependencies) == 1
    assert getattr(dependencies[0], "dependency").__name__ == "add_cache_headers"


def test_app_wires_require_json_on_post_search():
    paths, dependencies = app.route_dependencies[1]

    assert paths == [{"path": "/search", "method": "POST"}]
    assert len(dependencies) == 1
    assert getattr(dependencies[0], "dependency").__name__ == "require_json"


def test_require_json_passes_for_application_json():
    asyncio.run(app.require_json("application/json"))
    asyncio.run(app.require_json("application/json; charset=utf-8"))


@pytest.mark.parametrize("content_type", ["text/plain", "application/xml", ""])
def test_require_json_raises_415_for_non_json_content_type(content_type):
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(app.require_json(content_type))

    assert exc_info.value.status_code == 415


def test_run_calls_uvicorn_with_settings(monkeypatch):
    calls = []
    fake_uvicorn = SimpleNamespace(
        run=lambda *args, **kwargs: calls.append((args, kwargs))
    )
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)
    monkeypatch.setattr(
        app,
        "settings",
        SimpleNamespace(app_host="127.0.0.1", app_port=9000, reload=True),
    )

    app.run()

    assert calls == [
        (
            ("stac_fastapi.globus_search.app:app",),
            {
                "host": "127.0.0.1",
                "port": 9000,
                "log_level": "info",
                "reload": True,
            },
        )
    ]


def test_run_raises_runtime_error_when_uvicorn_is_missing(monkeypatch):
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "uvicorn":
            raise ImportError("missing uvicorn")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="Uvicorn must be installed"):
        app.run()
