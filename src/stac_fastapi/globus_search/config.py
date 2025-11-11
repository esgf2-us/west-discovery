import globus_sdk
from stac_fastapi.types.config import ApiSettings

# Index created by ALCF and populated with STAC items from netcdf files
SEARCH_INDEX_ID = "c01e0cd7-a479-43a0-9264-e484fa0f64c4"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient()
