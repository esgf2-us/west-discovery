import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from stac_fastapi.globus_search.extensions.aggregration.client import (
    GlobusSearchAggregationClient,
    find_first_non_alphanumeric,
)


def _request(path="/aggregations", host="api.example.org"):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"host", host.encode())],
            "query_string": b"",
            "scheme": "https",
            "server": (host, 443),
            "client": ("testclient", 50000),
        }
    )


class FakeDatabase:
    def __init__(self):
        self.calls = []

    def apply_collections_filter(self, search, collections):
        self.calls.append(("apply_collections_filter", collections))
        search["collections"] = collections
        return search

    def apply_cql2_filter(self, search, filter_expr):
        self.calls.append(("apply_cql2_filter", filter_expr))
        search["filter_expr"] = filter_expr
        return search


class FakeSearchClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post_search(self, index_id, search):
        self.calls.append((index_id, search))
        return self.response


def _client(database=None, search_response=None):
    client = GlobusSearchAggregationClient(database=database or FakeDatabase(), session=None)
    client.client = FakeSearchClient(
        search_response
        if search_response is not None
        else {"total": 0, "facet_results": []}
    )
    return client


@pytest.mark.parametrize(
    ("aggregation", "expected"),
    [
        ("cmip6_activity_id_frequency", ("_", 5)),
        ("cordex-cmip6_activity_id_frequency", ("-", 6)),
        ("totalcount", (None, -1)),
    ],
)
def test_find_first_non_alphanumeric(aggregation, expected):
    assert find_first_non_alphanumeric(aggregation) == expected


def test_get_aggregations_returns_global_default_aggregation():
    result = asyncio.run(_client().get_aggregations(request=_request()))

    assert result == {
        "type": "AggregationCollection",
        "aggregations": [{"name": "total_count", "data_type": "integer"}],
        "links": [
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://api.example.org/",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": "https://api.example.org/aggregations",
            },
        ],
    }


def test_get_aggregations_returns_collection_defaults_and_links():
    client = _client()

    result = asyncio.run(
        client.get_aggregations(collection_id="CMIP6", request=_request())
    )

    assert result["type"] == "AggregationCollection"
    assert result["aggregations"] == (
        client.CMIP6_DEFAULT_AGGREGATIONS + [{"name": "total_count", "data_type": "integer"}]
    )
    assert result["links"] == [
        {
            "rel": "root",
            "type": "application/json",
            "href": "https://api.example.org/",
        },
        {
            "rel": "collection",
            "type": "application/json",
            "href": "https://api.example.org/collections/CMIP6",
        },
        {
            "rel": "self",
            "type": "application/json",
            "href": "https://api.example.org/collections/CMIP6/aggregations",
        },
    ]


def test_aggregate_rejects_collection_id_and_collections_together():
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client().aggregate(
                aggregations=["total_count"],
                collections=["CMIP7"],
                collection_id="CMIP6",
                request=_request(),
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "Cannot specify both 'collection_id' and 'collections' parameters."
    )


@pytest.mark.parametrize("aggregations", [None, []])
def test_aggregate_rejects_missing_aggregations(aggregations):
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_client().aggregate(aggregations=aggregations, request=_request()))

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "No 'aggregations' found. Use '/aggregations' to return available aggregations"
    )


def test_aggregate_rejects_malformed_frequency_aggregation_name():
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client().aggregate(
                aggregations=["cmip6activityidfrequency"],
                request=_request(),
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "Character separating project and field not found in aggregation string."
    )


def test_aggregate_total_count_returns_search_total():
    database = FakeDatabase()
    client = _client(database, search_response={"total": 42, "facet_results": []})

    result = asyncio.run(
        client.aggregate(
            aggregations=["total_count"],
            collections=["CMIP6"],
            request=_request(),
        )
    )

    assert database.calls == [("apply_collections_filter", ["CMIP6"])]
    index_id, search = client.client.calls[0]
    assert index_id == "test-search-index"
    assert search["collections"] == ["CMIP6"]
    assert search["q"] == "*"
    assert search["limit"] == 0
    assert result == {
        "type": "AggregationCollection",
        "aggregations": [
            {"name": "total_count", "data_type": "integer", "value": 42}
        ],
        "links": [
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://api.example.org/",
            }
        ],
    }


def test_aggregate_request_can_drive_filter_collection_and_size():
    database = FakeDatabase()
    client = _client(database, search_response={"total": 5, "facet_results": []})
    aggregate_request = SimpleNamespace(
        filter_expr={"op": "=", "args": [{"property": "collection"}, "CMIP6"]},
        aggregations=["total_count"],
        collections=None,
        size=3,
    )

    result = asyncio.run(
        client.aggregate(
            aggregate_request=aggregate_request,
            request=_request(path="/collections/CMIP6/aggregations"),
        )
    )

    assert database.calls == [
        ("apply_cql2_filter", {"op": "=", "args": [{"property": "collection"}, "CMIP6"]}),
        ("apply_collections_filter", ["CMIP6"]),
    ]
    index_id, search = client.client.calls[0]
    assert index_id == "test-search-index"
    assert search["filter_expr"] == {
        "op": "=",
        "args": [{"property": "collection"}, "CMIP6"],
    }
    assert search["collections"] == ["CMIP6"]
    assert result["aggregations"][0]["value"] == 5
    assert result["links"] == [
        {
            "rel": "root",
            "type": "application/json",
            "href": "https://api.example.org/",
        },
        {
            "rel": "collection",
            "type": "application/json",
            "href": "https://api.example.org/collections/CMIP6",
        },
        {
            "rel": "self",
            "type": "application/json",
            "href": "https://api.example.org/collections/CMIP6/aggregations",
        },
    ]


def test_aggregate_request_without_filter_or_collection_path_uses_body_values():
    database = FakeDatabase()
    client = _client(database, search_response={"total": 9, "facet_results": []})
    aggregate_request = SimpleNamespace(
        filter_expr=None,
        aggregations=["total_count"],
        collections=["CMIP7"],
        size=4,
    )

    result = asyncio.run(
        client.aggregate(
            aggregate_request=aggregate_request,
            request=_request(path="/aggregations"),
        )
    )

    assert database.calls == [("apply_collections_filter", ["CMIP7"])]
    assert result["aggregations"][0]["value"] == 9
    assert result["links"] == [
        {
            "rel": "root",
            "type": "application/json",
            "href": "https://api.example.org/",
        }
    ]


def test_aggregate_adds_terms_facet_and_converts_buckets():
    client = _client(
        search_response={
            "total": 2,
            "facet_results": [
                {
                    "name": "activity_id",
                    "buckets": [
                        {"value": "CMIP", "count": 8},
                        {"value": "ScenarioMIP", "count": 4},
                    ],
                }
            ],
        }
    )

    result = asyncio.run(
        client.aggregate(
            aggregations=["cmip6_activity_id_frequency"],
            collection_id="CMIP6",
            size=2,
            request=_request(),
        )
    )

    index_id, search = client.client.calls[0]
    assert index_id == "test-search-index"
    assert search["collections"] == ["CMIP6"]
    assert search["facets"] == [
        {
            "name": "activity_id",
            "field_name": "properties.cmip6:activity_id",
            "type": "terms",
            "size": 2,
        }
    ]
    assert result == {
        "type": "AggregationCollection",
        "aggregations": [
            {
                "name": "cmip6_activity_id_frequency",
                "data_type": "frequency_distribution",
                "buckets": [
                    {
                        "key": "CMIP",
                        "data_type": "frequency_distribution",
                        "frequency": 8,
                    },
                    {
                        "key": "ScenarioMIP",
                        "data_type": "frequency_distribution",
                        "frequency": 4,
                    },
                ],
            }
        ],
        "links": [
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://api.example.org/",
            },
            {
                "rel": "collection",
                "type": "application/json",
                "href": "https://api.example.org/collections/CMIP6",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": "https://api.example.org/collections/CMIP6/aggregations",
            },
        ],
    }


def test_aggregate_returns_empty_aggregations_without_facet_results():
    client = _client(search_response={"total": 0, "facet_results": []})

    result = asyncio.run(
        client.aggregate(
            aggregations=["cmip6_activity_id_frequency"],
            request=_request(),
        )
    )

    assert result["aggregations"] == []
