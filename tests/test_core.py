import pytest
from unittest.mock import AsyncMock, MagicMock

from fastapi import HTTPException

import stac_fastapi.globus_search.core as core


@pytest.fixture
def fake_database():
    db = MagicMock()
    db.make_search.return_value = "mock-search"
    db.apply_collections_filter.return_value = "filtered-search"
    db.execute_search = AsyncMock(return_value=(
        [{"id": "item1", "links": [{"href": "http://localhost/next"}]}],
        1,
        "mock-marker",
    ))
    db.apply_ids_filter.return_value = "ids-filtered-search"
    db.apply_datetime_filter.return_value = "dt-filtered-search"
    db.apply_bbox_filter.return_value = "bbox-filtered-search"
    db.apply_intersects_filter.return_value = "intersects-filtered-search"
    db.apply_cql2_filter.return_value = "cql2-filtered-search"
    return db


@pytest.fixture
def fake_request():
    req = MagicMock()
    req.query_params.get.side_effect = lambda k, d=None: "token-value"
    req.url = "http://localhost/path"
    return req


@pytest.fixture
def fake_links():
    mock = AsyncMock()
    mock.get_links.return_value = [{"rel": "next", "href": "http://localhost/next"}]
    return mock


@pytest.mark.asyncio
async def test_item_collection(monkeypatch, fake_database, fake_request, fake_links):
    client = core.GlobusSearchClient(database=fake_database)
    # Patch PagingLinks to return mock links
    monkeypatch.setattr(core, "PagingLinks", lambda *args, **kwargs: fake_links)
    # Patch ItemCollection
    monkeypatch.setattr(core.stac_types, "ItemCollection", lambda **kwargs: kwargs)

    result = await client.item_collection("abc", limit=1, token=None, request=fake_request)
    assert result["features"][0]["id"] == "item1"
    assert result["links"][0]["href"] == "http://localhost/next"
    assert result["numReturned"] == 1
    assert client.database.execute_search.await_count == 1


@pytest.mark.asyncio
async def test_post_search_success(monkeypatch, fake_database, fake_request, fake_links):
    client = core.GlobusSearchClient(database=fake_database)

    class SearchRequest:
        ids = None
        collections = ["a"]
        datetime = None
        bbox = None
        intersects = None
        limit = 4
        token = "tok"
        filter = None

    search_request = SearchRequest()
    monkeypatch.setattr(core, "PagingLinks", lambda *args, **kwargs: fake_links)
    monkeypatch.setattr(core.stac_types, "ItemCollection", lambda **kwargs: kwargs)

    result = await client.post_search(search_request, fake_request)
    assert result["numReturned"] == 1


@pytest.mark.asyncio
async def test_post_search_cql2_error(monkeypatch, fake_database, fake_request, fake_links):
    client = core.GlobusSearchClient(database=fake_database)

    class SearchRequest:
        ids = None
        collections = None
        datetime = None
        bbox = None
        intersects = None
        limit = 3
        token = "tok"
        filter = "trigger-error"

    # Force apply_cql2_filter to raise
    fake_database.apply_cql2_filter.side_effect = Exception("bad filter")
    monkeypatch.setattr(core, "PagingLinks", lambda *args, **kwargs: fake_links)
    monkeypatch.setattr(core.stac_types, "ItemCollection", lambda **kwargs: kwargs)

    with pytest.raises(HTTPException) as exc:
        await client.post_search(SearchRequest(), fake_request)
    assert exc.value.status_code == 400
    assert "cql2_json" in exc.value.detail
