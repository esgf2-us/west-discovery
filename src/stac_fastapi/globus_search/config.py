import globus_sdk
from stac_fastapi.types.config import ApiSettings


# Index created by ALCF and populated with STAC items from netcdf files
SEARCH_INDEX_ID = "b9b74283-9465-41c3-884a-85188a484bc0"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient()
