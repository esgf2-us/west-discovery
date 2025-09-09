"""
This app definition is a fork of the one from the Mongo backend for
stac-fastapi.
"""

from typing import Optional

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import (create_get_request_model,
                                     create_post_request_model)
from stac_fastapi.core.basic_auth import apply_basic_auth
from stac_fastapi.core.core import CoreClient, EsAsyncBaseFiltersClient
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core import (FilterExtension, TokenPaginationExtension)

from stac_fastapi.types import stac as stac_types
from stac_pydantic.shared import BBox

# globus-search implementation
from stac_fastapi.globus_search.config import GlobusSearchSettings
from stac_fastapi.globus_search.database_logic import DatabaseLogic

settings = GlobusSearchSettings()
session = Session.create_from_settings(settings)

filter_extension = FilterExtension(client=EsAsyncBaseFiltersClient())
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)
pagination_extension = TokenPaginationExtension()

database_logic = DatabaseLogic()

extensions = [
    filter_extension,
    pagination_extension,
]

post_request_model = create_post_request_model(extensions)


class CustomizedCoreClient(CoreClient):
    async def item_collection(
        self,
        collection_id: str,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = 10,
        token: Optional[str] = None,
        filter_expr: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:

        request = kwargs.get("request")
        token = request.query_params.get("token", token)

        items, total, next_marker = await self.database.execute_search(
            search=self.database.make_search(),
            limit=limit,
            token=token,
            sort=None,
            collection_ids=[collection_id],
        )

        links = await PagingLinks(request=request, next=next_marker).get_links()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=total,
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
        )


api = StacApi(
    settings=settings,
    extensions=extensions,
    client=CustomizedCoreClient(
        database=database_logic, session=session, post_request_model=post_request_model
    ),
    pagination_extension=TokenPaginationExtension,
    search_get_request_model=create_get_request_model(extensions),
    search_post_request_model=post_request_model,
)
handler = api.app

apply_basic_auth(api)


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        print("host: ", settings.app_host)
        print("port: ", settings.app_port)
        uvicorn.run(
            "stac_fastapi.globus_search.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()
