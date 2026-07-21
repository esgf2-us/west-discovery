from typing import Optional
from urllib.parse import urljoin, urlparse

from fastapi import HTTPException
from stac_fastapi.core.core import CoreClient
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.requests import get_base_url
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes
from starlette.concurrency import run_in_threadpool

from stac_fastapi.globus_search.utility import list_project_summaries


class GlobusSearchClient(CoreClient):
    async def landing_page(self, **kwargs) -> stac_types.LandingPage:
        """Landing page."""
        request = kwargs["request"]
        base_url = get_base_url(request)
        landing_page = self._landing_page(
            base_url=base_url,
            conformance_classes=self.conformance_classes(),
            extension_schemas=[],
        )

        if self.extension_is_enabled("FilterExtension"):
            landing_page["links"].append(
                {
                    "rel": "queryables",
                    "type": "application/schema+json",
                    "title": "Queryables",
                    "href": urljoin(base_url, "queryables"),
                }
            )

        if self.extension_is_enabled("AggregationExtension"):
            landing_page["links"].extend(
                [
                    {
                        "rel": "aggregate",
                        "type": "application/json",
                        "title": "Aggregate",
                        "href": urljoin(base_url, "aggregate"),
                    },
                    {
                        "rel": "aggregations",
                        "type": "application/json",
                        "title": "Aggregations",
                        "href": urljoin(base_url, "aggregations"),
                    },
                ]
            )

        projects = await run_in_threadpool(list_project_summaries)
        for project in projects:
            landing_page["links"].append(
                {
                    "rel": Relations.child.value,
                    "type": MimeTypes.json.value,
                    "title": project.get("title") or project["id"],
                    "href": urljoin(base_url, f"collections/{project['id']}"),
                }
            )

        landing_page["links"].append(
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI service description",
                "href": urljoin(
                    str(request.base_url), request.app.openapi_url.lstrip("/")
                ),
            }
        )
        landing_page["links"].append(
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI service documentation",
                "href": urljoin(
                    str(request.base_url), request.app.docs_url.lstrip("/")
                ),
            }
        )

        return landing_page

    async def get_collection(
        self, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        collection = await super().get_collection(collection_id=collection_id, **kwargs)
        # Need to figure out a better way to do this —
        # the collection_id is embedded in the hrefs of
        # all links, so we need to update them to match
        # the case of the request
        for link in collection["links"]:
            if collection_id.lower() in link["href"]:
                if collection_id.lower() == "obs4ref":
                    new_collection_id = "obs4REF"
                elif collection_id.lower() == "obs4mips":
                    new_collection_id = "obs4MIPS"
                elif collection_id.lower() == "cmip6plus":
                    new_collection_id = "CMIP6Plus"
                else:
                    new_collection_id = collection_id.upper()
                link["href"] = link["href"].replace(
                    collection_id.lower(), new_collection_id
                )
        return collection

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
            item_links = item.get("links", [])
            for index, link in enumerate(item_links):
                if type(link) is dict:
                    link_href = urlparse(str(link.get("href", "")))
                    if "localhost" in request_url_href.netloc:
                        link_href = link_href._replace(scheme="http")
                    link_href = link_href._replace(netloc=request_url_href.netloc)
                    item_links[index]["href"] = link_href.geturl()

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
        if hasattr(search_request, "filter_expr"):
            cql2_filter = getattr(search_request, "filter_expr", None)
            try:
                search = self.database.apply_cql2_filter(search, cql2_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        # Free-text filter
        if hasattr(search_request, "q"):
            free_text_queries = getattr(search_request, "q", None)
            # Normalise: advanced extension delivers a raw string, basic delivers a list
            if isinstance(free_text_queries, str):
                free_text_queries = [t.strip() for t in free_text_queries.split(",")]
            if free_text_queries:
                try:
                    search = self.database.apply_free_text_filter(
                        search, free_text_queries
                    )
                except Exception as e:
                    raise HTTPException(
                        status_code=400, detail=f"Error with free-text query: {e}"
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
