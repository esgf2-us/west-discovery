import os
from typing import Any, Self

import globus_sdk
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from stac_fastapi.types.config import ApiSettings

DOTENV = os.path.join(os.path.dirname(__file__), ".env")


class SearchIndexSettings(BaseModel):
    search_client: Any
    search_index_id: str

    @model_validator(mode="before")
    @classmethod
    def pre_load(self, data: dict) -> Self:
        data["search_client"] = globus_sdk.SearchClient()


class Settings(BaseSettings):
    search_client: Any = globus_sdk.SearchClient()
    search_index_id: str

    model_config = SettingsConfigDict(
        env_file=DOTENV,
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()

class GlobusSearchSettings(ApiSettings):
    @property
    def create_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient()
