import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from stac_fastapi.core.extensions.filter import DEFAULT_QUERYABLES

from stac_fastapi.globus_search.filter import (GlobusSearchFiltersClient,
                                               _infer_json_schema_type)


def _make_database(items=None):
    """Return a mock DatabaseLogic that yields the given items from execute_search."""
    db = MagicMock()
    db.make_search.return_value = {}
    db.apply_collections_filter.side_effect = lambda search, ids: search
    db.execute_search = AsyncMock(return_value=(items or [], 0, None))
    return db


def _make_item(properties):
    return {"properties": properties}


@pytest.fixture
def db():
    return _make_database()


@pytest.fixture
def client(db):
    return GlobusSearchFiltersClient(database=db)


# --- _infer_json_schema_type ---


@pytest.mark.parametrize(
    ("value", "expected_type"),
    [
        ("hello", "string"),
        (42, "number"),
        (3.14, "number"),
        (True, "boolean"),
        (False, "boolean"),
        (["a", "b"], "array"),
        (None, "string"),
    ],
)
def test_infer_json_schema_type(value, expected_type):
    assert _infer_json_schema_type(value)["type"] == expected_type


def test_infer_json_schema_type_bool_not_treated_as_number():
    # bool is a subclass of int in Python — must be caught before int check
    result = _infer_json_schema_type(True)
    assert result["type"] == "boolean"


def test_infer_json_schema_type_array_has_string_items():
    result = _infer_json_schema_type(["x"])
    assert result == {"type": "array", "items": {"type": "string"}}


# --- Global queryables (no collection_id) ---


def test_global_queryables_returns_default_queryables(client):
    result = asyncio.run(client.get_queryables())

    assert result["properties"] == DEFAULT_QUERYABLES


def test_global_queryables_schema_structure(client):
    result = asyncio.run(client.get_queryables())

    assert result["$schema"] == "https://json-schema.org/draft/2019-09/schema"
    assert result["type"] == "object"
    assert result["additionalProperties"] is True


def test_global_queryables_includes_standard_stac_fields(client):
    result = asyncio.run(client.get_queryables())

    for field in ("id", "collection", "geometry", "datetime"):
        assert field in result["properties"]


def test_global_queryables_does_not_call_database(db):
    client = GlobusSearchFiltersClient(database=db)
    asyncio.run(client.get_queryables())

    db.execute_search.assert_not_called()


# --- Collection-scoped queryables ---


def test_collection_queryables_includes_default_queryables(client):
    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    for field in DEFAULT_QUERYABLES:
        assert field in result["properties"]


def test_collection_queryables_adds_item_properties(db):
    db.execute_search = AsyncMock(
        return_value=(
            [_make_item({"cmip6:activity_id": "CMIP", "cmip6:variable_id": "tas"})],
            1,
            None,
        )
    )
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert "cmip6:activity_id" in result["properties"]
    assert "cmip6:variable_id" in result["properties"]


def test_collection_queryables_infers_types_from_item_values(db):
    db.execute_search = AsyncMock(
        return_value=(
            [
                _make_item(
                    {
                        "cmip6:activity_id": "CMIP",
                        "cmip6:forcing_index": 1,
                        "cmip6:realm": ["atmos"],
                        "latest": True,
                    }
                )
            ],
            1,
            None,
        )
    )
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))
    props = result["properties"]

    assert props["cmip6:activity_id"]["type"] == "string"
    assert props["cmip6:forcing_index"]["type"] == "number"
    assert props["cmip6:realm"]["type"] == "array"
    assert props["latest"]["type"] == "boolean"


def test_collection_queryables_does_not_overwrite_default_queryables(db):
    db.execute_search = AsyncMock(
        return_value=(
            [
                _make_item(
                    {"datetime": "2020-01-01T00:00:00Z", "cmip6:variable_id": "tas"}
                )
            ],
            1,
            None,
        )
    )
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert result["properties"]["datetime"] == DEFAULT_QUERYABLES["datetime"]


def test_collection_queryables_schema_structure(client):
    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert result["$schema"] == "https://json-schema.org/draft/2019-09/schema"
    assert result["type"] == "object"
    assert result["additionalProperties"] is False
    assert "CMIP6" in result["title"]


def test_collection_queryables_id_uses_request_url_when_present(db):
    fake_request = SimpleNamespace(
        url="https://api.example.org/collections/CMIP6/queryables"
    )
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(
        client.get_queryables(collection_id="CMIP6", request=fake_request)
    )

    assert result["$id"] == "https://api.example.org/collections/CMIP6/queryables"


def test_collection_queryables_id_falls_back_to_placeholder_without_request(client):
    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert "CMIP6" in result["$id"]
    assert "example.com" in result["$id"]


def test_global_queryables_id_uses_request_url_when_present():
    fake_request = SimpleNamespace(url="https://api.example.org/queryables")
    client = GlobusSearchFiltersClient()

    result = asyncio.run(client.get_queryables(request=fake_request))

    assert result["$id"] == "https://api.example.org/queryables"


def test_collection_queryables_title_derived_from_key(db):
    db.execute_search = AsyncMock(
        return_value=(
            [_make_item({"cmip6:activity_id": "CMIP"})],
            1,
            None,
        )
    )
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert result["properties"]["cmip6:activity_id"]["title"] == "Cmip6:Activity Id"


def test_collection_queryables_passes_collection_id_to_database(db):
    client = GlobusSearchFiltersClient(database=db)
    asyncio.run(client.get_queryables(collection_id="CMIP6"))

    db.apply_collections_filter.assert_called_once_with({}, ["CMIP6"])


# --- No-database fallback ---


def test_collection_queryables_without_database_returns_default_queryables():
    client = GlobusSearchFiltersClient(database=None)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert result["properties"] == DEFAULT_QUERYABLES


# --- Error handling ---


def test_collection_queryables_handles_empty_collection(db):
    db.execute_search = AsyncMock(return_value=([], 0, None))
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert result["properties"] == DEFAULT_QUERYABLES


def test_collection_queryables_handles_search_exception(db):
    db.execute_search = AsyncMock(side_effect=Exception("search failed"))
    client = GlobusSearchFiltersClient(database=db)

    result = asyncio.run(client.get_queryables(collection_id="CMIP6"))

    assert result["properties"] == DEFAULT_QUERYABLES
