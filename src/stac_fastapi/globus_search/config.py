import os
from typing import Any

import globus_sdk
from pydantic_settings import BaseSettings, SettingsConfigDict

DOTENV = os.path.join(os.path.dirname(__file__), ".env")


class Settings(BaseSettings):
    search_client: Any = globus_sdk.SearchClient()
    search_index_id: str

    model_config = SettingsConfigDict(
        env_file=DOTENV,
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
