import asyncio
from types import SimpleNamespace

import pytest
from stac_fastapi.core.extensions.filter import DEFAULT_QUERYABLES

from stac_fastapi.globus_search import filter as filter_module
from stac_fastapi.globus_search.filter import GlobusSearchFiltersClient


def _make_prop(catalog_field_name, catalog_field_value_type, is_required=True):
    return SimpleNamespace(
        catalog_field_name=catalog_field_name,
        catalog_field_value_type=catalog_field_value_type,
        is_required=is_required,
    )


def _make_specs(dataset_properties):
    return SimpleNamespace(
        catalog_specs=SimpleNamespace(dataset_properties=dataset_properties),
    )


@pytest.fixture
def client():
    return GlobusSearchFiltersClient()


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


# --- Collection-scoped queryables ---


def test_collection_queryables_includes_default_queryables(client, monkeypatch):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: _make_specs([_make_prop("activity_id", "string")]),
    )

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))

    for field in DEFAULT_QUERYABLES:
        assert field in result["properties"]


def test_collection_queryables_adds_esgvoc_fields(client, monkeypatch):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: _make_specs(
            [
                _make_prop("activity_id", "string"),
                _make_prop("variable_id", "string"),
            ]
        ),
    )

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))

    assert "activity_id" in result["properties"]
    assert "variable_id" in result["properties"]


def test_collection_queryables_schema_structure(client, monkeypatch):
    monkeypatch.setattr(filter_module.ev, "get_project", lambda _: _make_specs([]))

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))

    assert result["$schema"] == "https://json-schema.org/draft/2019-09/schema"
    assert result["type"] == "object"
    assert result["additionalProperties"] is False
    assert "CMIP7" in result["$id"]
    assert "CMIP7" in result["title"]


# --- Type mapping ---


@pytest.mark.parametrize(
    ("value_type", "expected"),
    [
        ("string", {"type": "string", "title": "Activity Id"}),
        (
            "string_array",
            {"type": "array", "items": {"type": "string"}, "title": "Activity Id"},
        ),
        ("number", {"type": "number", "title": "Activity Id"}),
    ],
)
def test_collection_queryables_maps_value_types(
    client, monkeypatch, value_type, expected
):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: _make_specs([_make_prop("activity_id", value_type)]),
    )

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))

    assert result["properties"]["activity_id"] == expected


def test_collection_queryables_unknown_value_type_falls_back_to_string(
    client, monkeypatch
):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: _make_specs([_make_prop("activity_id", "unknown_type")]),
    )

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))

    assert result["properties"]["activity_id"]["type"] == "string"


# --- Field filtering ---


def test_collection_queryables_skips_props_with_no_field_name(client, monkeypatch):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: _make_specs(
            [
                _make_prop(None, "string"),
                _make_prop("activity_id", "string"),
            ]
        ),
    )

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))
    non_default_fields = set(result["properties"]) - set(DEFAULT_QUERYABLES)

    assert non_default_fields == {"activity_id"}


# --- Error handling ---


def test_collection_queryables_handles_missing_project_gracefully(client, monkeypatch):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: (_ for _ in ()).throw(ValueError("project not found")),
    )

    result = asyncio.run(client.get_queryables(collection_id="UNKNOWN"))

    assert result["properties"] == DEFAULT_QUERYABLES


def test_collection_queryables_handles_none_catalog_specs(client, monkeypatch):
    monkeypatch.setattr(
        filter_module.ev,
        "get_project",
        lambda _: SimpleNamespace(catalog_specs=None),
    )

    result = asyncio.run(client.get_queryables(collection_id="CMIP7"))

    assert result["properties"] == DEFAULT_QUERYABLES


def test_collection_queryables_passes_collection_id_to_esgvoc(client, monkeypatch):
    received = []

    def fake_get_project(collection_id):
        received.append(collection_id)
        return _make_specs([])

    monkeypatch.setattr(filter_module.ev, "get_project", fake_get_project)

    asyncio.run(client.get_queryables(collection_id="CMIP7"))

    assert received == ["cmip7"]
