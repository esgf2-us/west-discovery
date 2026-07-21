import asyncio

import globus_sdk
import pytest

from stac_fastapi.globus_search import database_logic
from stac_fastapi.globus_search.database_logic import (DatabaseLogic,
                                                       cql_like_to_globus_like,
                                                       cql_to_filter)


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("hist%", "hist*"),
        ("ta_", "ta?"),
        ("CMIP\\_%", "CMIP_*"),
    ],
)
def test_cql_like_to_globus_like(pattern, expected):
    assert cql_like_to_globus_like(pattern) == expected


@pytest.mark.parametrize(
    ("cql_query", "expected"),
    [
        ({}, {}),
        (
            {"op": "=", "args": [{"property": "collection"}, "CMIP6"]},
            {
                "type": "match_any",
                "field_name": "collection",
                "values": ["CMIP6"],
            },
        ),
        (
            {"op": "<>", "args": [{"property": "collection"}, "CMIP6"]},
            {
                "type": "not",
                "filter": {
                    "type": "match_any",
                    "field_name": "collection",
                    "values": ["CMIP6"],
                },
            },
        ),
        (
            {"op": "isNull", "args": [{"property": "properties.variable_id"}]},
            {
                "type": "not",
                "filter": {
                    "type": "exists",
                    "field_name": "properties.variable_id",
                },
            },
        ),
        (
            {"op": "<=", "args": [{"property": "properties.datetime"}, "2026-01-01"]},
            {
                "type": "range",
                "field_name": "properties.datetime",
                "values": [{"from": "*", "to": "2026-01-01"}],
            },
        ),
        (
            {"op": ">=", "args": [{"property": "properties.datetime"}, "2025-01-01"]},
            {
                "type": "range",
                "field_name": "properties.datetime",
                "values": [{"from": "2025-01-01", "to": "*"}],
            },
        ),
        (
            {"op": "in", "args": [{"property": "collection"}, ["CMIP6", "CMIP7"]]},
            {
                "type": "match_any",
                "field_name": "collection",
                "values": ["CMIP6", "CMIP7"],
            },
        ),
        (
            {
                "op": "s_intersects",
                "args": [
                    {"property": "geometry"},
                    {"type": "Point", "coordinates": [0, 1]},
                ],
            },
            {
                "type": "geo_shape",
                "field_name": "geometry",
                "relation": "intersects",
                "shape": {"type": "Point", "coordinates": [0, 1]},
            },
        ),
        (
            {
                "op": "s_within",
                "args": [
                    {"property": "geometry"},
                    {"type": "Polygon", "coordinates": []},
                ],
            },
            {
                "type": "geo_shape",
                "field_name": "geometry",
                "relation": "within",
                "shape": {"type": "Polygon", "coordinates": []},
            },
        ),
    ],
)
def test_cql_to_filter_translates_supported_filters(cql_query, expected):
    assert cql_to_filter(cql_query) == expected


def test_cql_to_filter_translates_boolean_groups():
    cql_query = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "collection"}, "CMIP6"]},
            {
                "op": "in",
                "args": [{"property": "properties.variable_id"}, ["tas", "pr"]],
            },
        ],
    }

    assert cql_to_filter(cql_query) == {
        "type": "and",
        "filters": [
            {
                "type": "match_any",
                "field_name": "collection",
                "values": ["CMIP6"],
            },
            {
                "type": "match_any",
                "field_name": "properties.variable_id",
                "values": ["tas", "pr"],
            },
        ],
    }


def test_cql_to_filter_translates_not_and_collapses_double_negative():
    positive = {"op": "=", "args": [{"property": "collection"}, "CMIP6"]}
    negative = {"op": "not", "args": [positive]}

    assert cql_to_filter(negative) == {
        "type": "not",
        "filter": {
            "type": "match_any",
            "field_name": "collection",
            "values": ["CMIP6"],
        },
    }
    assert cql_to_filter({"op": "not", "args": [negative]}) == {
        "type": "match_any",
        "field_name": "collection",
        "values": ["CMIP6"],
    }


@pytest.mark.parametrize(
    "operator",
    [
        "<",
        ">",
        "between",
        "s_contains",
        "s_disjoint",
        "t_after",
        "t_before",
        "t_disjoint",
        "t_equals",
        "t_intersects",
        "t_contains",
        "t_during",
        "t_finishedby",
        "t_finishes",
        "t_meets",
        "t_metby",
        "t_overlappedby",
        "t_overlaps",
        "t_startedby",
        "t_starts",
        "unknown",
    ],
)
def test_cql_to_filter_raises_not_implemented_for_unsupported_filters(operator):
    with pytest.raises(NotImplementedError):
        cql_to_filter({"op": operator, "args": [{"property": "x"}, "y"]})


@pytest.mark.parametrize(
    "operator",
    [
        "s_crosses",
        "s_equals",
        "s_overlaps",
        "s_touches",
        "a_equals",
        "a_contains",
        "a_contained_by",
        "a_overlaps",
        "casei",
        "accenti",
        "+",
        "-",
        "*",
        "/",
        "%",
        "div",
        "^",
    ],
)
def test_cql_to_filter_raises_value_error_for_rejected_filters(operator):
    with pytest.raises(ValueError):
        cql_to_filter({"op": operator, "args": [{"property": "x"}, "y"]})


def test_apply_ids_filter_adds_match_any_filter():
    search = globus_sdk.SearchQuery()

    returned = DatabaseLogic.apply_ids_filter(search, ["item-1", "item-2"])

    assert returned is search
    assert search["filters"] == [
        {
            "field_name": "id",
            "values": ["item-1", "item-2"],
            "type": "match_any",
        }
    ]


def test_apply_collections_filter_adds_match_any_filter():
    search = globus_sdk.SearchQuery()

    returned = DatabaseLogic.apply_collections_filter(search, ["CMIP6"])

    assert returned is search
    assert search["filters"] == [
        {
            "field_name": "collection",
            "values": ["CMIP6"],
            "type": "match_any",
        }
    ]


def test_apply_intersects_filter_is_currently_noop():
    search = globus_sdk.SearchQuery()
    shape = {"type": "Point", "coordinates": [0, 1]}

    returned = DatabaseLogic.apply_intersects_filter(search, shape)

    assert returned is search
    assert search == {}


def test_apply_bbox_filter_adds_geo_bounding_box_filter():
    search = globus_sdk.SearchQuery()

    returned = DatabaseLogic.apply_bbox_filter(search, [-10, -20, 30, 40])

    assert returned is search
    assert search["filters"] == [
        {
            "type": "geo_bounding_box",
            "field_name": "geometry",
            "top_left": {"lat": 40, "lon": -10},
            "bottom_right": {"lat": -20, "lon": 30},
        }
    ]


def test_apply_bbox_filter_preserves_existing_filters():
    search = globus_sdk.SearchQuery()
    search["filters"] = [{"type": "exists", "field_name": "id"}]

    DatabaseLogic.apply_bbox_filter(search, [-10, -20, 30, 40])

    assert search["filters"][0] == {"type": "exists", "field_name": "id"}
    assert search["filters"][1]["type"] == "geo_bounding_box"


def test_apply_cql2_filter_appends_translated_filter():
    search = globus_sdk.SearchQuery()

    returned = DatabaseLogic.apply_cql2_filter(
        search, {"op": "=", "args": [{"property": "collection"}, "CMIP6"]}
    )

    assert returned is search
    assert search["filters"] == [
        {
            "type": "match_any",
            "field_name": "collection",
            "values": ["CMIP6"],
        }
    ]


def test_apply_cql2_filter_leaves_search_unchanged_without_filter():
    search = globus_sdk.SearchQuery()

    returned = DatabaseLogic.apply_cql2_filter(search, None)

    assert returned is search
    assert search == {}


def test_apply_free_text_filter_sets_or_joined_query():
    search = globus_sdk.SearchScrollQuery()

    returned = DatabaseLogic.apply_free_text_filter(search, ["tas", "precip"])

    assert returned is search
    assert search["q"] == "tas OR precip"


@pytest.mark.parametrize("free_text_queries", [None, []])
def test_apply_free_text_filter_leaves_search_unchanged_without_queries(
    free_text_queries,
):
    search = globus_sdk.SearchScrollQuery()

    returned = DatabaseLogic.apply_free_text_filter(search, free_text_queries)

    assert returned is search
    assert search == {}


def test_find_collection_returns_project(monkeypatch):
    monkeypatch.setattr(
        database_logic,
        "get_project",
        lambda collection_id: {"id": collection_id},
    )

    result = asyncio.run(DatabaseLogic().find_collection("CMIP6"))

    assert result == {"id": "CMIP6"}


def test_get_all_collections_returns_projects(monkeypatch):
    monkeypatch.setattr(
        database_logic,
        "list_projects",
        lambda: ([{"id": "CMIP6"}], None),
    )

    result = asyncio.run(
        DatabaseLogic().get_all_collections(token=None, limit=10, request=None)
    )

    assert result == ([{"id": "CMIP6"}], None)


def test_get_one_item_fetches_subject_and_converts_result(monkeypatch):
    class FakeResponse:
        data = {"subject": "item-1"}

    class FakeClient:
        def get_subject(self, index_id, item_id):
            assert index_id == "test-search-index"
            assert item_id == "item-1"
            return FakeResponse()

    monkeypatch.setattr(database_logic, "_client", FakeClient())
    monkeypatch.setattr(
        database_logic,
        "search_doc_to_stac_item",
        lambda doc: {"id": doc["subject"]},
    )

    result = asyncio.run(DatabaseLogic().get_one_item("CMIP6", "item-1"))

    assert result == {"id": "item-1"}


def test_make_search_returns_search_scroll_query():
    assert isinstance(DatabaseLogic.make_search(), globus_sdk.SearchScrollQuery)


def test_execute_search_sets_defaults_and_converts_results(monkeypatch):
    class FakeClient:
        def scroll(self, index_id, search):
            assert index_id == "test-search-index"
            assert search["q"] == "*"
            assert search["limit"] == 2
            return {
                "gmeta": ["doc-1", "doc-2"],
                "total": 20,
                "marker": "next-marker",
            }

    monkeypatch.setattr(database_logic, "_client", FakeClient())
    monkeypatch.setattr(
        database_logic,
        "search_doc_to_stac_item",
        lambda doc: {"id": doc},
    )

    result = asyncio.run(
        DatabaseLogic().execute_search(
            search=globus_sdk.SearchScrollQuery(),
            limit=2,
            token=None,
            sort=None,
            collection_ids=None,
        )
    )

    assert result == ([{"id": "doc-1"}, {"id": "doc-2"}], 20, "next-marker")


def test_execute_search_preserves_existing_query(monkeypatch):
    class FakeClient:
        def scroll(self, index_id, search):
            assert search["q"] == "tas"
            return {"gmeta": [], "total": 0, "marker": None}

    monkeypatch.setattr(database_logic, "_client", FakeClient())

    result = asyncio.run(
        DatabaseLogic().execute_search(
            search=globus_sdk.SearchScrollQuery(q="tas"),
            limit=10,
            token=None,
            sort=None,
            collection_ids=None,
        )
    )

    assert result == ([], 0, None)


def test_execute_search_does_not_set_default_query_when_filters_exist(monkeypatch):
    class FakeClient:
        def scroll(self, index_id, search):
            assert "q" not in search
            assert search["filters"] == [{"type": "exists", "field_name": "id"}]
            return {"gmeta": [], "total": 0, "marker": None}

    search = globus_sdk.SearchScrollQuery()
    search["filters"] = [{"type": "exists", "field_name": "id"}]
    monkeypatch.setattr(database_logic, "_client", FakeClient())

    result = asyncio.run(
        DatabaseLogic().execute_search(
            search=search,
            limit=10,
            token=None,
            sort=None,
            collection_ids=None,
        )
    )

    assert result == ([], 0, None)


def test_execute_search_sets_pagination_token(monkeypatch):
    class FakeClient:
        def scroll(self, index_id, search):
            assert search["marker"] == "page-2"
            return {"gmeta": [], "total": 0, "marker": None}

    monkeypatch.setattr(database_logic, "_client", FakeClient())

    result = asyncio.run(
        DatabaseLogic().execute_search(
            search=globus_sdk.SearchScrollQuery(),
            limit=10,
            token="page-2",
            sort=None,
            collection_ids=None,
        )
    )

    assert result == ([], 0, None)
