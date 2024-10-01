#!/usr/bin/env pip-run
# /// script
# dependencies = ["pystac-client"]
# ///

import json

import pystac_client

ITEM_LIMIT = 1
CEDA_STAC_URL = "https://api.stac.ceda.ac.uk/"

stac_client = pystac_client.Client.open(CEDA_STAC_URL)


def iter_items():
    search_results = stac_client.search(
        collections=["cmip6"],
        fields={"exclude": [], "include": ["properties"]},
    )
    for item in search_results.items_as_dicts():
        yield item


for i, item in enumerate(iter_items()):
    if i >= ITEM_LIMIT:
        break
    print(json.dumps(item, indent=2, separators=(",", ": "), sort_keys=True))


# vim: ft=python
