"""Custom filter client for the Globus Search backend.

Derives queryable schemas from esgvoc project metadata rather than
Elasticsearch index mappings, which don't exist in this backend.
"""

import logging
from typing import Any, Dict, Optional

import attr
import esgvoc.api.projects as ev
from stac_fastapi.core.extensions.filter import DEFAULT_QUERYABLES
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient

logger = logging.getLogger(__name__)

_VALUE_TYPE_TO_JSON_SCHEMA: dict[str, dict] = {
    "string": {"type": "string"},
    "string_array": {"type": "array", "items": {"type": "string"}},
    "number": {"type": "number"},
}


@attr.s
class GlobusSearchFiltersClient(AsyncBaseFiltersClient):
    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        properties: Dict[str, Any] = dict(DEFAULT_QUERYABLES)

        if not collection_id:
            return {
                "$schema": "https://json-schema.org/draft/2019-09/schema",
                "$id": "https://stac-api.example.com/queryables",
                "type": "object",
                "title": "Queryables for STAC API",
                "description": "Queryable names for the STAC API Item Search filter.",
                "properties": properties,
                "additionalProperties": True,
            }

        try:
            specs = ev.get_project(collection_id.lower())
        except Exception:
            logger.warning("No esgvoc specs found for collection '%s'", collection_id)
            specs = None

        if specs and specs.catalog_specs:
            for prop in specs.catalog_specs.dataset_properties:
                if not prop.catalog_field_name:
                    continue
                field_schema = dict(
                    _VALUE_TYPE_TO_JSON_SCHEMA.get(
                        prop.catalog_field_value_type, {"type": "string"}
                    )
                )
                field_schema["title"] = prop.catalog_field_name.replace(
                    "_", " "
                ).title()
                properties[prop.catalog_field_name] = field_schema

        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": f"https://stac-api.example.com/collections/{collection_id}/queryables",
            "type": "object",
            "title": f"Queryables for {collection_id}",
            "description": (
                f"Queryable names for the STAC API Item Search filter "
                f"scoped to the {collection_id} collection."
            ),
            "properties": properties,
            "additionalProperties": False,
        }
