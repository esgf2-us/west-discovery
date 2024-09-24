"""
This app definition is a fork of the one from the Mongo backend for
stac-fastapi.
"""

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.core.basic_auth import apply_basic_auth
from stac_fastapi.core.core import CoreClient, EsAsyncBaseFiltersClient
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core import FilterExtension

# globus-search implementation
from stac_fastapi.globus_search.config import GlobusSearchSettings
from stac_fastapi.globus_search.database_logic import DatabaseLogic

settings = GlobusSearchSettings()
session = Session.create_from_settings(settings)

filter_extension = FilterExtension(client=EsAsyncBaseFiltersClient())
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)

database_logic = DatabaseLogic()

extensions = [
    filter_extension,
    # QueryExtension(),
]

post_request_model = create_post_request_model(extensions)


class CustomizedCoreClient(CoreClient):
    async def post_search(self, search_request, request):
        # dumb hack to work around stac-fastapi-core not supporting applications
        # with extensions disabled
        # see:
        # https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/263
        object.__setattr__(search_request, "query", None)
        object.__setattr__(search_request, "sortby", None)
        object.__setattr__(search_request, "token", None)
        return await super().post_search(search_request, request)


api = StacApi(
    settings=settings,
    extensions=extensions,
    client=CustomizedCoreClient(
        database=database_logic, session=session, post_request_model=post_request_model
    ),
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
