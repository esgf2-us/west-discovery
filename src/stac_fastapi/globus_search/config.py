import globus_sdk
from stac_fastapi.types.config import ApiSettings


# Index created by ALCF and populated with STAC items from netcdf files
# SEARCH_INDEX_ID = "d7814ff7-51a9-4155-8b84-97e84600acd7"
SEARCH_INDEX_ID = "f037bb33-3413-448b-8486-8400bee5181a"


class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient()
