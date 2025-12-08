from typing import Optional
from urllib.parse import urlparse

from fastapi import HTTPException
from stac_fastapi.core.core import CoreClient
from stac_fastapi.core.models.links import PagingLinks

from stac_fastapi.types import stac as stac_types


class GlobusSearchClient(CoreClient):
    async def item_collection(
        self,
        collection_id: str,
        limit: Optional[int] = 10,
        token: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:

        request = kwargs.get("request")
        search = self.database.make_search()
        token = request.query_params.get("token", token)

        if collection_id:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=[collection_id]
            )

        items, total, next_marker = await self.database.execute_search(
            search=search,
            limit=limit,
            token=token,
            sort=None,
            collection_ids=[collection_id],
        )

        links = await PagingLinks(request=request, next=next_marker).get_links()

        # Fix item hrefs to match request host
        request_url_href = urlparse(str(request.url))
        for item in items:
            links = item.get("links", [])
            for index, link in enumerate(links):
                link_href = urlparse(str(link.get("href", "")))
                if "localhost" in request_url_href.netloc:
                    link_href = link_href._replace(scheme="http")
                link_href = link_href._replace(netloc=request_url_href.netloc)
                links[index]["href"] = link_href.geturl()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=total,
            context={"matched": total},
        )

    async def post_search(self, search_request, request):
        # Hack to work around stac-fastapi-core not supporting applications
        # with extensions disabled
        # see:
        # https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/263
        object.__setattr__(search_request, "query", None)
        object.__setattr__(search_request, "sortby", None)

        search = self.database.make_search()

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if search_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            search = self.database.apply_intersects_filter(
                search=search, intersects=search_request.intersects
            )

        # only cql2_json is supported here
        if hasattr(search_request, "filter"):
            cql2_filter = getattr(search_request, "filter", None)
            try:
                search = self.database.apply_cql2_filter(search, cql2_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        # Extract pagination parameters
        limit = getattr(search_request, "limit", 10)
        token = getattr(search_request, "token", None)

        items, total, next_marker = await self.database.execute_search(
            search=search,
            limit=limit,
            token=token,
            sort=None,
            collection_ids=search_request.collections,
        )

        links = await PagingLinks(request=request, next=next_marker).get_links()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=total,
            context={"matched": total},
        )
