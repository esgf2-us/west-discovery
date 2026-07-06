from stac_fastapi.globus_search.convert import search_doc_to_stac_item


def _search_doc_with_assets(assets):
    return {
        "entries": [
            {
                "content": {
                    "id": "item-1",
                    "collection": "CMIP6",
                    "assets": assets,
                }
            }
        ]
    }


def test_search_doc_to_stac_item_converts_asset_list_to_name_keyed_dict():
    search_doc = _search_doc_with_assets(
        [
            {
                "name": "data",
                "href": "https://example.org/data.nc",
                "type": "application/netcdf",
            },
            {
                "name": "metadata",
                "href": "https://example.org/metadata.json",
                "type": "application/json",
            },
        ]
    )

    item = search_doc_to_stac_item(search_doc)

    assert item["assets"] == {
        "data": {
            "href": "https://example.org/data.nc",
            "type": "application/netcdf",
        },
        "metadata": {
            "href": "https://example.org/metadata.json",
            "type": "application/json",
        },
    }


def test_search_doc_to_stac_item_converts_alternates_to_name_keyed_dict():
    search_doc = _search_doc_with_assets(
        [
            {
                "name": "data",
                "href": "https://example.org/data.nc",
                "alternate": [
                    {
                        "name": "s3",
                        "href": "s3://bucket/data.nc",
                    },
                    {
                        "name": "https",
                        "href": "https://mirror.example.org/data.nc",
                    },
                ],
            }
        ]
    )

    item = search_doc_to_stac_item(search_doc)

    assert item["assets"] == {
        "data": {
            "href": "https://example.org/data.nc",
            "alternate": {
                "s3": {"href": "s3://bucket/data.nc"},
                "https": {"href": "https://mirror.example.org/data.nc"},
            },
        }
    }


def test_search_doc_to_stac_item_skips_nameless_alternates():
    search_doc = _search_doc_with_assets(
        [
            {
                "name": "data",
                "href": "https://example.org/data.nc",
                "alternate": [
                    {"name": "s3", "href": "s3://bucket/data.nc"},
                    {"href": "https://mirror.example.org/data.nc"},
                ],
            }
        ]
    )

    item = search_doc_to_stac_item(search_doc)

    assert item["assets"]["data"]["alternate"] == {
        "s3": {"href": "s3://bucket/data.nc"}
    }


def test_search_doc_to_stac_item_preserves_non_asset_content_fields():
    search_doc = _search_doc_with_assets(
        [
            {
                "name": "data",
                "href": "https://example.org/data.nc",
            }
        ]
    )
    search_doc["entries"][0]["content"]["properties"] = {"variable_id": "tas"}

    item = search_doc_to_stac_item(search_doc)

    assert item["id"] == "item-1"
    assert item["collection"] == "CMIP6"
    assert item["properties"] == {"variable_id": "tas"}
