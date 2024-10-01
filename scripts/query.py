#!/usr/bin/env pip-run
# /// script
# dependencies = ["pystac-client"]
# ///
from __future__ import annotations

import json
import typing as t

import pystac_client

LOCAL_STAC_URL = "http://localhost:8000/"

# AOI around Delfzijl, in northern Netherlands
aoi_as_dict: dict[str, t.Any] = {
    "type": "Polygon",
    "coordinates": [[[6, 53], [7, 53], [7, 54], [6, 54], [6, 53]]],
}


def main():
    stac_client = pystac_client.Client.open(LOCAL_STAC_URL)
    query = stac_client.search(
        # filter={"op": ">=", "args": [{"property": "eo:cloud_cover"}, 10]},
        limit=2,
        collections="cmip6",
        # intersects=aoi_as_dict,
        bbox=[53, 6, 54, 7],
    )
    print(json.dumps(list(query.items_as_dicts()), indent=2, separators=(",", ": ")))


if __name__ == "__main__":
    main()


# vim: ft=python
