"""
This app definition is a fork of the one from the Mongo backend for
stac-fastapi.
"""

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import (create_get_request_model,
                                     create_post_request_model)
from stac_fastapi.core.extensions.aggregation import (
    EsAggregationExtensionGetRequest,
    EsAggregationExtensionPostRequest,
)
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core import (
    AggregationExtension,
    FilterExtension,
    TokenPaginationExtension
)
from stac_fastapi.globus_search.config import GlobusSearchSettings
from stac_fastapi.globus_search.core import GlobusSearchClient
from stac_fastapi.globus_search.database_logic import DatabaseLogic
from stac_fastapi.globus_search.extensions.aggregration.client import GlobusSearchAggregationClient
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient


database_logic = DatabaseLogic()
settings = GlobusSearchSettings()
session = Session.create_from_settings(settings)

aggregation_extension = AggregationExtension(
    client=GlobusSearchAggregationClient(
        database=database_logic, session=session, settings=settings
    )
)
aggregation_extension.POST = EsAggregationExtensionPostRequest
aggregation_extension.GET = EsAggregationExtensionGetRequest

filter_extension = FilterExtension(
    client=EsAsyncBaseFiltersClient(database=database_logic)
)
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)
pagination_extension = TokenPaginationExtension()

search_extensions = [
    filter_extension,
    pagination_extension,
]

post_request_model = create_post_request_model(search_extensions)

extensions = [aggregation_extension] + search_extensions
    
api = StacApi(
    settings=settings,
    extensions=extensions,
    client=GlobusSearchClient(
        database=database_logic, session=session, post_request_model=post_request_model
    ),
    search_get_request_model=create_get_request_model(search_extensions),
    search_post_request_model=post_request_model,
)
handler = api.app


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
