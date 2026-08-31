"""Custom filter client for the Globus Search backend.

Derives queryable schemas from esgvoc (authoritative CV source) when the
collection is a known ESGF project, falling back to sampling a real item
from the index for unrecognised collections.
"""

import logging
from functools import lru_cache
from typing import Any, Dict, Optional

import attr
import esgvoc.api.projects as ev
from esgvoc.apps.jsg.json_schema_generator import generate_json_schema
from stac_fastapi.core.extensions.filter import DEFAULT_QUERYABLES
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def _build_esgvoc_queryables(collection_id: str) -> Dict[str, Any]:
    project_id = collection_id.lower()
    if ev.get_project(project_id) is None:
        return {}

    schema = generate_json_schema(project_id)
    item_properties = (
        schema.get("definitions", {}).get("item_fields", {}).get("properties", {})
    )

    result = {}
    for key, field_schema in item_properties.items():
        if key in DEFAULT_QUERYABLES:
            continue
        result[key] = {
                "type": field_schema.get("type", "string"),
                "title": key.replace("_", " ").title(),
            }

    return result


def _infer_json_schema_type(value: Any) -> dict:
    # bool must be checked before int — isinstance(True, int) is True in Python
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, (int, float)):
        return {"type": "number"}
    if isinstance(value, list):
        return {"type": "array", "items": {"type": "string"}}
    return {"type": "string"}


@attr.s
class GlobusSearchFiltersClient(AsyncBaseFiltersClient):
    database: Any = attr.ib(default=None)

    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        properties: Dict[str, Any] = dict(DEFAULT_QUERYABLES)
        request = kwargs.get("request")
        request_url = str(request.url) if request is not None else None

        if not collection_id:
            return {
                "$schema": "https://json-schema.org/draft/2019-09/schema",
                "$id": request_url or "https://stac-api.example.com/queryables",
                "type": "object",
                "title": "Queryables for STAC API",
                "description": "Queryable names for the STAC API Item Search filter.",
                "properties": properties,
                "additionalProperties": True,
            }

        esgvoc_props = self._esgvoc_item_properties(collection_id)
        if esgvoc_props:
            properties.update(esgvoc_props)
        elif self.database is not None:
            sampled = await self._sample_item_properties(collection_id)
            properties.update(sampled)

        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": request_url or f"https://stac-api.example.com/collections/{collection_id}/queryables",
            "type": "object",
            "title": f"Queryables for {collection_id}",
            "description": (
                f"Queryable names for the STAC API Item Search filter "
                f"scoped to the {collection_id} collection."
            ),
            "properties": properties,
            "additionalProperties": False,
        }

    def _esgvoc_item_properties(self, collection_id: str) -> Dict[str, Any]:
        try:
            return _build_esgvoc_queryables(collection_id)
        except Exception:
            logger.warning(
                "Failed to get esgvoc queryables for collection '%s'", collection_id
            )
            return {}

    async def _sample_item_properties(self, collection_id: str) -> Dict[str, Any]:
        search = self.database.make_search()
        search = self.database.apply_collections_filter(search, [collection_id])
        try:
            items, _, _ = await self.database.execute_search(
                search=search,
                limit=1,
                token=None,
                sort=None,
                collection_ids=[collection_id],
            )
        except Exception:
            logger.warning(
                "Failed to sample item for collection '%s' queryables", collection_id
            )
            return {}

        if not items:
            return {}

        item_properties = items[0].get("properties", {})
        result = {}
        for key, value in item_properties.items():
            if key in DEFAULT_QUERYABLES:
                continue
            field_schema = _infer_json_schema_type(value)
            field_schema["title"] = key.replace("_", " ").title()
            result[key] = field_schema

        return result
