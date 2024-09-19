import json


def search_doc_to_stac_item(search_doc):
    assert len(search_doc["entries"]) == 1
    content = dict(search_doc["entries"][0]["content"])
    content["assets"] = json.loads(content["assets"])
    return content
