def search_doc_to_stac_item(search_doc):
    assert len(search_doc["entries"]) == 1

    # Convert assets from list to dict
    content = dict(search_doc["entries"][0]["content"])
    dict_assets = {}
    list_assets = content["assets"]
    for asset in list_assets:
        dict_assets[asset["name"]] = asset
    content["assets"] = dict_assets

    return content
