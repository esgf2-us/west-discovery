import globus_sdk
from stac_fastapi.types.config import ApiSettings

# Index created by ALCF and populated with STAC items from netcdf files
SEARCH_INDEX_ID = "8bf113fd-4cf2-4ed0-8e27-12dac07b6e1b"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient()
