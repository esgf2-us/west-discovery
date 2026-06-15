import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from stac_fastapi.globus_search import core as core_module
from stac_fastapi.globus_search.core import GlobusSearchClient


def _request(
    path="/search",
    query_string=b"",
    host="api.example.org",
    scheme="https",
):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"host", host.encode())],
            "query_string": query_string,
            "scheme": scheme,
            "server": (host, 443),
            "client": ("testclient", 50000),
        }
    )


class FakeDatabase:
    def __init__(self):
        self.calls = []
        self.items = []
        self.total = 0
        self.next_marker = None

    def make_search(self):
        self.calls.append(("make_search",))
        return {"filters": []}

    def apply_ids_filter(self, search, item_ids):
        self.calls.append(("apply_ids_filter", item_ids))
        search["ids"] = item_ids
        return search

    def apply_collections_filter(self, search, collection_ids):
        self.calls.append(("apply_collections_filter", collection_ids))
        search["collections"] = collection_ids
        return search

    def apply_datetime_filter(self, search, datetime_search):
        self.calls.append(("apply_datetime_filter", datetime_search))
        search["datetime"] = datetime_search
        return search

    def apply_bbox_filter(self, search, bbox):
        self.calls.append(("apply_bbox_filter", bbox))
        search["bbox"] = bbox
        return search

    def apply_intersects_filter(self, search, intersects):
        self.calls.append(("apply_intersects_filter", intersects))
        search["intersects"] = intersects
        return search

    def apply_cql2_filter(self, search, filter_):
        self.calls.append(("apply_cql2_filter", filter_))
        search["filter_expr"] = filter_
        return search

    def apply_free_text_filter(self, search, free_text_queries):
        self.calls.append(("apply_free_text_filter", free_text_queries))
        search["q"] = free_text_queries
        return search

    async def execute_search(self, **kwargs):
        self.calls.append(("execute_search", kwargs))
        return self.items, self.total, self.next_marker


def _client(database=None):
    return GlobusSearchClient(database=database or FakeDatabase())


@pytest.mark.parametrize(
    ("collection_id", "expected"),
    [
        ("obs4ref", "obs4REF"),
        ("obs4mips", "obs4MIPS"),
        ("cmip6plus", "CMIP6Plus"),
        ("cmip6", "CMIP6"),
    ],
)
def test_get_collection_rewrites_collection_link_casing(
    monkeypatch, collection_id, expected
):
    async def fake_get_collection(self, collection_id, **kwargs):
        return {
            "id": collection_id,
            "links": [
                {
                    "rel": "self",
                    "href": f"https://example.org/collections/{collection_id.lower()}",
                },
                {
                    "rel": "root",
                    "href": "https://example.org/",
                },
            ],
        }

    monkeypatch.setattr(
        core_module.CoreClient,
        "get_collection",
        fake_get_collection,
    )

    collection = asyncio.run(
        _client().get_collection(collection_id, request=_request())
    )

    assert collection["links"][0]["href"] == (
        f"https://example.org/collections/{expected}"
    )
    assert collection["links"][1]["href"] == "https://example.org/"


def test_item_collection_applies_collection_filter_and_query_token():
    database = FakeDatabase()
    database.items = [{"id": "item-1", "links": []}]
    database.total = 12
    database.next_marker = "next-page"

    item_collection = asyncio.run(
        _client(database).item_collection(
            "CMIP6",
            limit=25,
            token="argument-token",
            request=_request(
                path="/collections/CMIP6/items",
                query_string=b"token=query-token",
            ),
        )
    )

    assert ("apply_collections_filter", ["CMIP6"]) in database.calls
    execute_call = database.calls[-1]
    assert execute_call[0] == "execute_search"
    assert execute_call[1]["limit"] == 25
    assert execute_call[1]["token"] == "query-token"
    assert execute_call[1]["collection_ids"] == ["CMIP6"]
    assert item_collection["type"] == "FeatureCollection"
    assert item_collection["features"] == [{"id": "item-1", "links": []}]
    assert item_collection["numReturned"] == 1
    assert item_collection["numMatched"] == 12
    assert item_collection["context"] == {"matched": 12}
    assert item_collection["links"][0]["rel"] == "next"


def test_item_collection_rewrites_item_link_hosts_without_replacing_paging_links():
    database = FakeDatabase()
    database.items = [
        {
            "id": "item-1",
            "links": [
                {"rel": "self", "href": "https://old.example.org/items/item-1"},
                "not-a-dict",
            ],
        }
    ]
    database.total = 1
    database.next_marker = "next-page"

    item_collection = asyncio.run(
        _client(database).item_collection(
            "CMIP6",
            request=_request(path="/collections/CMIP6/items"),
        )
    )

    assert item_collection["features"][0]["links"][0]["href"] == (
        "https://api.example.org/items/item-1"
    )
    assert item_collection["features"][0]["links"][1] == "not-a-dict"
    assert item_collection["links"][0]["rel"] == "next"


def test_item_collection_uses_http_scheme_for_localhost_item_links():
    database = FakeDatabase()
    database.items = [
        {
            "id": "item-1",
            "links": [{"rel": "self", "href": "https://old.example.org/items/1"}],
        }
    ]

    item_collection = asyncio.run(
        _client(database).item_collection(
            "CMIP6",
            request=_request(
                path="/collections/CMIP6/items",
                host="localhost:8000",
                scheme="http",
            ),
        )
    )

    assert item_collection["features"][0]["links"][0]["href"] == (
        "http://localhost:8000/items/1"
    )


def test_item_collection_without_collection_id_skips_collection_filter():
    database = FakeDatabase()

    asyncio.run(
        _client(database).item_collection(
            "",
            request=_request(path="/collections/items"),
        )
    )

    assert not any(call[0] == "apply_collections_filter" for call in database.calls)
    execute_call = database.calls[-1]
    assert execute_call[1]["collection_ids"] == [""]


def test_post_search_applies_request_filters_and_pagination():
    database = FakeDatabase()
    database.items = [{"id": "item-1"}]
    database.total = 7
    database.next_marker = "next-page"
    client = _client(database)
    client._return_date = lambda value: f"parsed:{value}"
    search_request = SimpleNamespace(
        ids=["item-1"],
        collections=["CMIP6"],
        datetime="2025-01-01/2025-12-31",
        bbox=[-10, -20, 0, 30, 40, 0],
        intersects={"type": "Point", "coordinates": [0, 0]},
        filter_expr={"op": "=", "args": [{"property": "collection"}, "CMIP6"]},
        q="tas, pr",
        limit=50,
        token="page-2",
    )

    item_collection = asyncio.run(
        client.post_search(search_request, _request(path="/search"))
    )

    assert ("apply_ids_filter", ["item-1"]) in database.calls
    assert ("apply_collections_filter", ["CMIP6"]) in database.calls
    assert (
        "apply_datetime_filter",
        "parsed:2025-01-01/2025-12-31",
    ) in database.calls
    assert ("apply_bbox_filter", [-10, -20, 30, 40]) in database.calls
    assert (
        "apply_intersects_filter",
        {"type": "Point", "coordinates": [0, 0]},
    ) in database.calls
    assert (
        "apply_cql2_filter",
        {"op": "=", "args": [{"property": "collection"}, "CMIP6"]},
    ) in database.calls
    assert ("apply_free_text_filter", ["tas", "pr"]) in database.calls
    execute_call = database.calls[-1]
    assert execute_call[0] == "execute_search"
    assert execute_call[1]["limit"] == 50
    assert execute_call[1]["token"] == "page-2"
    assert execute_call[1]["collection_ids"] == ["CMIP6"]
    assert item_collection["features"] == [{"id": "item-1"}]
    assert item_collection["numMatched"] == 7
    assert item_collection["links"][0]["rel"] == "next"
    assert search_request.query is None
    assert search_request.sortby is None


def test_post_search_uses_four_value_bbox_without_rewriting():
    database = FakeDatabase()
    search_request = SimpleNamespace(
        ids=None,
        collections=None,
        datetime=None,
        bbox=[-10, -20, 30, 40],
        intersects=None,
    )

    asyncio.run(_client(database).post_search(search_request, _request(path="/search")))

    assert ("apply_bbox_filter", [-10, -20, 30, 40]) in database.calls
    assert not any(call[0] == "apply_cql2_filter" for call in database.calls)
    assert not any(call[0] == "apply_free_text_filter" for call in database.calls)


def test_post_search_accepts_free_text_list():
    database = FakeDatabase()
    search_request = SimpleNamespace(
        ids=None,
        collections=None,
        datetime=None,
        bbox=None,
        intersects=None,
        q=["tas", "pr"],
    )

    asyncio.run(_client(database).post_search(search_request, _request(path="/search")))

    assert ("apply_free_text_filter", ["tas", "pr"]) in database.calls


@pytest.mark.parametrize("q", [None, []])
def test_post_search_skips_empty_free_text(q):
    database = FakeDatabase()
    search_request = SimpleNamespace(
        ids=None,
        collections=None,
        datetime=None,
        bbox=None,
        intersects=None,
        q=q,
    )

    asyncio.run(_client(database).post_search(search_request, _request(path="/search")))

    assert not any(call[0] == "apply_free_text_filter" for call in database.calls)


def test_post_search_wraps_cql2_filter_errors_in_http_400():
    class BadFilterDatabase(FakeDatabase):
        def apply_cql2_filter(self, search, filter_):
            raise ValueError("bad cql")

    search_request = SimpleNamespace(
        ids=None,
        collections=None,
        datetime=None,
        bbox=None,
        intersects=None,
        filter_expr={"op": "bad"},
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client(BadFilterDatabase()).post_search(
                search_request, _request(path="/search")
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Error with cql2_json filter: bad cql"


def test_post_search_wraps_free_text_errors_in_http_400():
    class BadFreeTextDatabase(FakeDatabase):
        def apply_free_text_filter(self, search, free_text_queries):
            raise ValueError("bad q")

    search_request = SimpleNamespace(
        ids=None,
        collections=None,
        datetime=None,
        bbox=None,
        intersects=None,
        q=["bad"],
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _client(BadFreeTextDatabase()).post_search(
                search_request, _request(path="/search")
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Error with free-text query: bad q"
