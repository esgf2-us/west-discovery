"""
Globus Search-based aggregation client for STAC FastAPI.

This module provides aggregation functionality using Globus Search's native
faceting capabilities, replacing ElasticSearch-based aggregations.
"""

from typing import Any, Optional
from urllib.parse import urljoin

import attrs
import globus_sdk
from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core.aggregation.client import BaseAggregationClient
from stac_fastapi.extensions.core.aggregation.types import AggregationCollection
from starlette.requests import Request

from stac_fastapi.globus_search.config import SEARCH_INDEX_ID, GlobusSearchSettings


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
        size: Optional[int] = 10,
        **kwargs,
    ) -> dict[str, Any]:
        request: Request = kwargs.get("request")
        base_url = str(request.base_url)

        if request.method == "POST":
            request = await request.json()
            # Expects the json body to have an "aggregations" list field
            aggregations = request.get("aggregations", [])

        links = [{"rel": "root", "type": "application/json", "href": base_url}]
        stac_aggregations = []

        search = globus_sdk.SearchQuery()
        search.set_query("*")
        search.set_limit(0)

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
            else:
                facet = aggregation.removeprefix("cmip6_").removesuffix("_frequency")
                search.add_facet(
                    facet,
                    field_name=f"properties.cmip6:{facet}", 
                    type="terms",
                    size=size
                )

        response = self.client.post_search(SEARCH_INDEX_ID, search)

        if response["facet_results"]:
            for facet in response["facet_results"]:
                stac_buckets = []
                for bucket in facet["buckets"]:
                    stac_bucket = {
                        "key": bucket["value"],
                        "data_type": "frequency_distribution",
                        "frequency": bucket["count"]
                    }
                    stac_buckets.append(stac_bucket)
                stac_aggregation = {
                    "name": f"cmip6_{facet['name']}_frequency",
                    "data_type": "frequency_distribution",
                    "buckets": stac_buckets
                }
                stac_aggregations.append(stac_aggregation)

        return {
            "type": "AggregationCollection",
            "aggregations": stac_aggregations,
            "links": links,
        }