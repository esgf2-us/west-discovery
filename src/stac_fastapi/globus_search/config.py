import globus_sdk
from stac_fastapi.types.config import ApiSettings

SEARCH_INDEX_ID = "0b717a04-73e4-47f8-a94d-1eddd8a07d49"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient(environment="integration")
