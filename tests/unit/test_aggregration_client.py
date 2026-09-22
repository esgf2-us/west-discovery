import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from stac_fastapi.globus_search.extensions.aggregration.client import (
    GlobusSearchAggregationClient,
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
    client = GlobusSearchAggregationClient(
        database=database or FakeDatabase(), session=None
    )
    client.client = FakeSearchClient(
        search_response
        if search_response is not None
        else {"total": 0, "facet_results": []}
    )
    return client


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
        client.DEFAULT_FREQUENCY_AGGREGATIONS
        + [{"name": "total_count", "data_type": "integer"}]
    )
    # Advertised names are bare (no project/collection prefix).
    assert "activity_id_frequency" in {
        a["name"] for a in client.DEFAULT_FREQUENCY_AGGREGATIONS
    }
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


def test_aggregate_rejects_namespaced_aggregation_without_single_collection():
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client().aggregate(
                aggregations=["activity_id_frequency"],
                request=_request(),
            )
        )

    assert exc_info.value.status_code == 400
    assert "requires exactly one collection" in exc_info.value.detail


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
        "aggregations": [{"name": "total_count", "data_type": "integer", "value": 42}],
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

    # The collection filter must be applied before the CQL2 filter so the CQL2
    # translation can resolve per-collection field namespaces (see the
    # collection-scoped regression test below).
    assert database.calls == [
        ("apply_collections_filter", ["CMIP6"]),
        (
            "apply_cql2_filter",
            {"op": "=", "args": [{"property": "collection"}, "CMIP6"]},
        ),
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


def test_aggregate_supports_common_alternate_name_frequency():
    client = _client(
        search_response={
            "total": 3,
            "facet_results": [
                {
                    "name": "alternate_name",
                    "buckets": [
                        {"value": "eagle.alcf.anl.gov", "count": 3},
                    ],
                }
            ],
        }
    )

    result = asyncio.run(
        client.aggregate(
            aggregations=["alternate_name_frequency"],
            collection_id="CMIP6Test",
            size=5,
            request=_request(),
        )
    )

    index_id, search = client.client.calls[0]
    assert index_id == "test-search-index"
    assert search["facets"] == [
        {
            "name": "alternate_name",
            "field_name": "assets.alternate:name",
            "type": "terms",
            "size": 5,
        }
    ]
    assert result["aggregations"][0]["name"] == "alternate_name_frequency"
    assert result["aggregations"][0]["buckets"][0]["key"] == "eagle.alcf.anl.gov"


def test_aggregate_returns_empty_aggregations_without_facet_results():
    client = _client(search_response={"total": 0, "facet_results": []})

    result = asyncio.run(
        client.aggregate(
            aggregations=["activity_id_frequency"],
            collection_id="CMIP6",
            request=_request(),
        )
    )

    assert result["aggregations"] == []


@pytest.mark.parametrize(
    "aggregation",
    [
        "activity_id_frequency",  # canonical bare name
        "cordex-cmip6_activity_id_frequency",  # legacy hyphen-prefixed name
        "cordex_cmip6_activity_id_frequency",  # legacy underscore-prefixed name
    ],
)
def test_aggregate_cordex_cmip6_resolves_collection_namespaced_field(aggregation):
    client = _client(
        search_response={
            "total": 1,
            "facet_results": [
                {
                    "name": "activity_id",
                    "buckets": [{"value": "DD", "count": 1}],
                }
            ],
        }
    )

    result = asyncio.run(
        client.aggregate(
            aggregations=[aggregation],
            collections=["CORDEX-CMIP6"],
            size=5,
            request=_request(),
        )
    )

    _, search = client.client.calls[0]
    assert search["facets"] == [
        {
            "name": "activity_id",
            "field_name": "properties.cordex-cmip6:activity_id",
            "type": "terms",
            "size": 5,
        }
    ]
    # Response echoes the name the client requested.
    assert result["aggregations"][0]["name"] == aggregation
    assert result["aggregations"][0]["buckets"][0]["key"] == "DD"


def test_aggregate_collection_scoped_filter_is_applied_with_collection():
    """A CQL2 filter on the collection-scoped /aggregate path is applied to the
    search alongside the collection filter. Field names resolve transparently:
    the "cordex-cmip6:" prefix is part of the property key the caller supplies,
    not injected from the URL (see database_logic.cql_translate_fieldname).
    Uses the real DatabaseLogic so the translation is exercised end to end.
    """
    from stac_fastapi.globus_search.database_logic import DatabaseLogic

    client = _client(DatabaseLogic(), search_response={"total": 7, "facet_results": []})
    aggregate_request = SimpleNamespace(
        filter_expr={
            "op": "=",
            "args": [{"property": "cordex-cmip6:frequency"}, "mon"],
        },
        aggregations=["total_count"],
        collections=None,
        size=10,
    )

    result = asyncio.run(
        client.aggregate(
            aggregate_request=aggregate_request,
            request=_request(path="/collections/CORDEX-CMIP6/aggregate"),
        )
    )

    _, search = client.client.calls[0]
    filters = search["filters"]
    # Collection filter is present...
    assert {
        "type": "match_any",
        "field_name": "collection",
        "values": ["CORDEX-CMIP6"],
    } in filters
    # ...and the property resolved by transparent "properties." prefixing.
    assert {
        "type": "match_any",
        "field_name": "properties.cordex-cmip6:frequency",
        "values": ["mon"],
    } in filters
    assert result["aggregations"][0]["value"] == 7


def test_aggregate_wraps_cql2_filter_errors_in_http_400():
    class BadFilterDatabase(FakeDatabase):
        def apply_cql2_filter(self, search, filter_expr):
            raise ValueError("bad filter")

    aggregate_request = SimpleNamespace(
        filter_expr={"op": "="},
        aggregations=["total_count"],
        collections=None,
        size=10,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client(BadFilterDatabase()).aggregate(
                aggregate_request=aggregate_request,
                request=_request(),
            )
        )

    assert exc_info.value.status_code == 400
    assert "Malformed CQL2 filter" in exc_info.value.detail


def test_aggregate_wraps_cql2_filter_not_implemented_in_http_501():
    class BadFilterDatabase(FakeDatabase):
        def apply_cql2_filter(self, search, filter_expr):
            raise NotImplementedError("not supported")

    aggregate_request = SimpleNamespace(
        filter_expr={"op": "t_after"},
        aggregations=["total_count"],
        collections=None,
        size=10,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client(BadFilterDatabase()).aggregate(
                aggregate_request=aggregate_request,
                request=_request(),
            )
        )

    assert exc_info.value.status_code == 501
    assert exc_info.value.detail == "not supported"
