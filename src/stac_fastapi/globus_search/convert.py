# STAC properties that must stay present even when null. `datetime` is required
# by the item spec and is legitimately null for items whose time is given via
# start_datetime/end_datetime, so it must not be pruned.
_PROPERTIES_KEEP_NULL = frozenset({"datetime"})


def search_doc_to_stac_item(search_doc):
    # Convert assets from list to dict
    content = dict(search_doc["entries"][0]["content"])

    # Source records (from CEDA, ingested verbatim) carry explicit nulls for
    # optional fields; STAC types many of these as arrays/objects and rejects a
    # null (e.g. properties.providers). These fields are optional, so drop the
    # null-valued ones rather than emit an invalid value. `datetime` is kept
    # because the spec requires it to be present even when null.
    properties = content.get("properties")
    if isinstance(properties, dict):
        for key in [
            k
            for k, v in properties.items()
            if v is None and k not in _PROPERTIES_KEEP_NULL
        ]:
            properties.pop(key)

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
