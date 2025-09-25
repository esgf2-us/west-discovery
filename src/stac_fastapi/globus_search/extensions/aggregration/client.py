"""
Globus Search-based aggregation client for STAC FastAPI.

This module provides aggregation functionality using Globus Search's native
faceting capabilities, replacing ElasticSearch-based aggregations.
"""

from typing import Any, Optional
from urllib.parse import urlencode, urljoin

import attrs
import globus_sdk
from fastapi import HTTPException
from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core.aggregation.client import BaseAggregationClient
from stac_fastapi.extensions.core.aggregation.types import AggregationCollection
from starlette.requests import Request

from stac_fastapi.globus_search.config import SEARCH_INDEX_ID, GlobusSearchSettings
from stac_fastapi.globus_search.database_logic import cql_to_filter


@attrs.define
class GlobusSearchAggregationClient(BaseAggregationClient):
    """
    Aggregation client that uses Globus Search facets for STAC aggregations.
    
    This client provides compatibility with the STAC aggregation extension
    by translating aggregation requests to Globus Search facet queries.
    """
    database: BaseDatabaseLogic = attrs.field()
    session: Session = attrs.field() 
    settings: GlobusSearchSettings = attrs.field()

    CMIP6_DEFAULT_AGGREGATIONS = [
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_activity_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_cf_standard_name_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_data_specs_version_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_experiment_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_experiment_title_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_frequency_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_further_info_url_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_grid_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_grid_label_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_institution_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_institution_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_mip_era_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_nominal_resolution_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_source_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "array",
            "name": "cmip6_source_type_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_sub_experiment_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_table_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_variable_id_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_variable_long_name_frequency",
            "data_type": "frequency_distribution"
        },
        {
            "frequency_distribution_data_type": "string",
            "name": "cmip6_variant_label_frequency",
            "data_type": "frequency_distribution"
        }
    ]

    DEFAULT_AGGREGATIONS = [
        {"name": "total_count", "data_type": "integer"},
    ]
    
    def __init__(self, *args, **kwargs):
        self.client = globus_sdk.SearchClient()

    async def get_aggregations(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> AggregationCollection:
        request: Request = kwargs.get("request")
        base_url = str(request.base_url) if request else ""
        links = [{"rel": "root", "type": "application/json", "href": base_url}]

        if collection_id is not None:
            collection_endpoint = urljoin(base_url, f"collections/{collection_id}")
            links.extend(
                [
                    {
                        "rel": "collection",
                        "type": "application/json",
                        "href": collection_endpoint,
                    },
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": urljoin(collection_endpoint + "/", "aggregations"),
                    },
                ]
            )
            aggregations = self.CMIP6_DEFAULT_AGGREGATIONS.copy() + self.DEFAULT_AGGREGATIONS.copy()
        else:
            links.append(
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": urljoin(base_url, "aggregations"),
                }
            )
            aggregations = self.DEFAULT_AGGREGATIONS.copy()

        return {
            "type": "AggregationCollection",
            "aggregations": aggregations,
            "links": links,
        }

    async def aggregate(
        self,
        aggregations: Optional[str] = None,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> dict[str, Any]:
        
        request: Request = kwargs.get("request")
        base_url = str(request.base_url) if request else ""
        links = [{"rel": "root", "type": "application/json", "href": base_url}]
        search = globus_sdk.SearchQuery()

        if collection_id is not None:
            collection_endpoint = urljoin(base_url, f"collections/{collection_id}")
            links.extend(
                [
                    {
                        "rel": "collection",
                        "type": "application/json",
                        "href": collection_endpoint,
                    },
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": urljoin(collection_endpoint + "/", "aggregations"),
                    },
                ]
            )

        for aggregation in aggregations:
            if aggregation == "total_count":
                
                search.set_query("*")
                response = self.client.post_search(SEARCH_INDEX_ID, search)

                return {
                    "type": "AggregationCollection",
                    "aggregations": [
                        {
                            "name": "total_count", 
                            "data_type": "integer", 
                            "value": response["total"]
                        }
                    ],
                    "links": links,
                }
        
        return {
            "type": "AggregationCollection",
            "aggregations": [],
            "links": links,
        }

        # # Build the base search query
        # search_query = self._build_base_search(collections, **kwargs)

        # # Convert aggregation to Globus Search facets
        # facets = self._convert_aggregation_to_facets(aggregations)
        
        # # Add facets to search query
        # search_query["facets"] = facets
        
        # # Execute the search with facets
        # try:
        #     response = self._client.post_search(SEARCH_INDEX_ID, search_query)
            
        #     # Convert facet results back to STAC aggregation format
        #     return self._convert_facet_results_to_aggregation(
        #         response.get("facet_results", []),
        #         aggregations
        #     )
        # except globus_sdk.SearchAPIError as e:
        #     raise HTTPException(
        #         status_code=400, 
        #         detail=f"Aggregation query failed: {e}"
        #     )

    def _build_base_search(
        self, 
        collections: list[str] | None = None,
        bbox: list[float] | None = None,
        datetime_filter: str | None = None,
        cql_filter: dict[str, Any] | None = None,
        **kwargs
    ) -> dict[str, Any]:
        """Build base search query with common filters."""
        query = {
            "q": "*",
            "filters": []
        }
        
        # Add collection filter
        if collections:
            query["filters"].append({
                "type": "match_any",
                "field_name": "collection",
                "values": collections
            })
        
        # Add bbox filter  
        if bbox:
            west, south, east, north = bbox[:4]  # Handle 2D bbox
            query["filters"].append({
                "type": "geo_bounding_box",
                "field_name": "geometry",
                "top_left": {"lat": north, "lon": west},
                "bottom_right": {"lat": south, "lon": east}
            })
        
        # Add datetime filter
        if datetime_filter:
            # Parse datetime and convert to appropriate filter
            dt_filter = self._parse_datetime_filter(datetime_filter)
            if dt_filter:
                query["filters"].append(dt_filter)
        
        # Add CQL2 filter
        if cql_filter:
            try:
                converted_filter = cql_to_filter(cql_filter)
                if converted_filter:
                    query["filters"].append(converted_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Error processing CQL filter: {e}"
                )
        
        return query

    def _convert_aggregation_to_facets(self, aggregation: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Convert STAC aggregation definition to Globus Search facets.
        
        Supports common aggregation patterns like:
        - Terms aggregation (distinct values)
        - Date histogram aggregation
        - Numeric histogram aggregation
        - Statistical aggregations (sum, avg)
        """
        facets = []
        
        for agg_name, agg_def in aggregation.items():
            facet = self._convert_single_aggregation_to_facet(agg_name, agg_def)
            if facet:
                facets.append(facet)
        
        return facets

    def _convert_single_aggregation_to_facet(
        self, 
        name: str, 
        agg_def: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert a single aggregation definition to a Globus Search facet."""
        
        agg_type = agg_def.get("type")
        field = agg_def.get("field", name)
        
        # Handle different aggregation types
        if agg_type == "terms":
            return {
                "name": name,
                "type": "terms",
                "field_name": self._normalize_field_name(field),
                "size": agg_def.get("size", 10)
            }
        
        elif agg_type == "date_histogram":
            facet = {
                "name": name,
                "type": "date_histogram", 
                "field_name": self._normalize_field_name(field),
                "date_interval": agg_def.get("interval", "day")
            }
            
            # Add histogram range if specified
            if "min_doc_count" in agg_def or "extended_bounds" in agg_def:
                bounds = agg_def.get("extended_bounds", {})
                if bounds:
                    facet["histogram_range"] = {
                        "low": bounds.get("min"),
                        "high": bounds.get("max")
                    }
            
            return facet
        
        elif agg_type == "histogram":
            return {
                "name": name,
                "type": "numeric_histogram",
                "field_name": self._normalize_field_name(field),
                "size": agg_def.get("interval", 10),
                "histogram_range": {
                    "low": agg_def.get("min_value", 0),
                    "high": agg_def.get("max_value", 1000000)
                }
            }
        
        elif agg_type in ["sum", "avg"]:
            facet = {
                "name": name,
                "type": agg_type,
                "field_name": self._normalize_field_name(field)
            }
            
            if "missing" in agg_def:
                facet["missing"] = agg_def["missing"]
            
            return facet
        
        # Handle nested aggregations by flattening them
        elif "aggs" in agg_def or "aggregations" in agg_def:
            nested_aggs = agg_def.get("aggs", agg_def.get("aggregations", {}))
            # For now, convert the first nested aggregation
            for nested_name, nested_def in nested_aggs.items():
                return self._convert_single_aggregation_to_facet(
                    f"{name}_{nested_name}", nested_def
                )
        
        return None

    def _normalize_field_name(self, field: str) -> str:
        """
        Normalize field names for Globus Search.
        
        Maps common STAC fields to their Globus Search equivalents
        and handles property field mapping.
        """
        # Direct field mappings
        field_mappings = {
            "id": "id",
            "collection": "collection", 
            "geometry": "geometry",
            "datetime": "properties.datetime",
        }
        
        if field in field_mappings:
            return field_mappings[field]
        
        # Properties are prefixed in Globus Search
        if not field.startswith(("properties.", "id", "collection", "geometry")):
            return f"properties.{field}"
        
        # Escape dots in field names as required by Globus Search
        return field.replace(".", "\\.")

    def _parse_datetime_filter(self, datetime_str: str) -> dict[str, Any] | None:
        """Parse datetime string into Globus Search datetime filter."""
        try:
            if "/" in datetime_str:
                # Range format: start/end
                start, end = datetime_str.split("/")
                return {
                    "type": "range",
                    "field_name": "properties.datetime",
                    "gte": start if start != ".." else None,
                    "lte": end if end != ".." else None
                }
            else:
                # Single datetime
                return {
                    "type": "match_any",
                    "field_name": "properties.datetime", 
                    "values": [datetime_str]
                }
        except Exception:
            return None

    def _convert_facet_results_to_aggregation(
        self, 
        facet_results: list[dict[str, Any]], 
        original_aggregation: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Convert Globus Search facet results back to STAC aggregation format.
        """
        aggregations = {}
        
        for facet_result in facet_results:
            name = facet_result["name"]
            
            # Handle bucket-based results (terms, histograms)
            if "buckets" in facet_result:
                buckets = []
                for bucket in facet_result["buckets"]:
                    buckets.append({
                        "key": bucket["value"],
                        "doc_count": bucket["count"]
                    })
                
                aggregations[name] = {
                    "buckets": buckets,
                    "sum_other_doc_count": 0,
                    "doc_count_error_upper_bound": 0
                }
            
            # Handle value-based results (sum, avg)
            elif "value" in facet_result:
                aggregations[name] = {
                    "value": facet_result["value"]
                }
        
        return {"aggregations": aggregations}

    async def post_aggregation(
        self,
        aggregation: dict[str, Any],
        collection_ids: list[str] | None = None,
        **search_params,
    ) -> dict[str, Any]:
        """
        Handle POST aggregation requests.
        
        This is typically the same as get_aggregation but allows for
        more complex request bodies.
        """
        return await self.get_aggregation(
            aggregation=aggregation,
            collection_ids=collection_ids, 
            **search_params
        )

    def get_aggregation_link(
        self, 
        aggregation: dict[str, Any],
        request: Request
    ) -> str:
        """Generate a link for the aggregation query."""
        params = {"aggregation": aggregation}
        query_string = urlencode(params)
        return f"{request.url.replace(query=query_string)}"