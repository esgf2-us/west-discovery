def search_doc_to_stac_item(search_doc):
    # Convert assets from list to dict
    content = dict(search_doc["entries"][0]["content"])

    dict_assets = {}
    list_assets = content["assets"]
    for asset in list_assets:
        for key in list(asset):
            value = asset[key]

            if key == "name":
                asset.pop(key)
                dict_assets[value] = asset

            if key == "alternate":
                temp = {}
                for alternate in value:
                    if "name" in alternate:
                        temp[alternate["name"]] = alternate
                        alternate.pop("name")

                asset["alternate"] = temp


    content["assets"] = dict_assets

    return content
