import globus_sdk
from stac_fastapi.types.config import ApiSettings

# Initial search index from the search team
# SEARCH_INDEX_ID = "0b717a04-73e4-47f8-a94d-1eddd8a07d49"

# Index created by ALCF and populated with STAC items from netcdf files
SEARCH_INDEX_ID = "d7814ff7-51a9-4155-8b84-97e84600acd7"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        # return globus_sdk.SearchClient(environment="integration")
        return globus_sdk.SearchClient()
