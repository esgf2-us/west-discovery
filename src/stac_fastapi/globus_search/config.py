import globus_sdk
from stac_fastapi.types.config import ApiSettings

# Index created by ALCF and populated with STAC items from netcdf files
SEARCH_INDEX_ID = "06e2ca28-a0ed-467c-8b91-ed8151e7e578"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient()
