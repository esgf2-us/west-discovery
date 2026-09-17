"""
Globus Search-based aggregation client for STAC FastAPI.

This module provides aggregation functionality using Globus Search's native
faceting capabilities, replacing ElasticSearch-based aggregations.
"""

from typing import Any, Optional
from urllib.parse import urljoin

import attrs
import globus_sdk
from fastapi import HTTPException
from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.extensions.aggregation import EsAggregationExtensionPostRequest
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core.aggregation.client import BaseAggregationClient
from stac_fastapi.extensions.core.aggregation.types import AggregationCollection
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request

from stac_fastapi.globus_search.config import settings
from stac_fastapi.globus_search.database_logic import _collection_property_prefix


@attrs.define
class GlobusSearchAggregationClient(BaseAggregationClient):
    """
    Aggregation client that uses Globus Search facets for STAC aggregations.

    This client provides compatibility with the STAC aggregation extension
    by translating aggregation requests to Globus Search facet queries.
    """

    client = settings.search_client
    database: BaseDatabaseLogic = attrs.field()
    session: Session = attrs.field()

    # Frequency-distribution facets shared by every ESGF project collection.
    #
    # Names are intentionally *bare* (no project/collection prefix). The
    # collection that scopes a request determines the index-field namespace
    # ("properties.{collection}:{facet}"), exactly like the CQL2 filter path
    # (see database_logic.cql_translate_fieldname). This mirrors the CEDA / east
    # (stac-fastapi-elasticsearch-opensearch) convention, where aggregation
    # names carry no project prefix. "source_type" is the only array facet.
    _STRING_FREQUENCY_FACETS = [
        "activity_id",
        "cf_standard_name",
        "data_specs_version",
        "experiment_id",
        "experiment_title",
        "frequency",
        "further_info_url",
        "grid",
        "grid_label",
        "institution",
        "institution_id",
        "mip_era",
        "nominal_resolution",
        "source_id",
        "sub_experiment_id",
        "table_id",
        "variable_id",
        "variable_long_name",
        "variant_label",
    ]
    _ARRAY_FREQUENCY_FACETS = ["source_type"]

    DEFAULT_FREQUENCY_AGGREGATIONS = (
        [
            {
                "frequency_distribution_data_type": "string",
                "name": "alternate_name_frequency",
                "data_type": "frequency_distribution",
            }
        ]
        + [
            {
                "frequency_distribution_data_type": "string",
                "name": f"{facet}_frequency",
                "data_type": "frequency_distribution",
            }
            for facet in _STRING_FREQUENCY_FACETS
        ]
        + [
            {
                "frequency_distribution_data_type": "array",
                "name": f"{facet}_frequency",
                "data_type": "frequency_distribution",
            }
            for facet in _ARRAY_FREQUENCY_FACETS
        ]
    )

    # Every known collection currently advertises the same facet set, so they
    # all point at the shared list above. Per-project facets (e.g.
    # CORDEX-CMIP6's domain / domain_id / driving_* / version_realisation
    # fields) should eventually be sourced from each collection's esgvoc
    # metadata rather than hard-coded here; see get_aggregations().
    COLLECTION_DEFAULT_AGGREGATIONS = {
        "CMIP6": DEFAULT_FREQUENCY_AGGREGATIONS,
        "CMIP7": DEFAULT_FREQUENCY_AGGREGATIONS,
        "CORDEX-CMIP6": DEFAULT_FREQUENCY_AGGREGATIONS,
        "obs4REF": DEFAULT_FREQUENCY_AGGREGATIONS,
    }
    DEFAULT_AGGREGATIONS = [
        {"name": "total_count", "data_type": "integer"},
    ]

    # Aggregations that map to a common (non-project-namespaced) index field
    # rather than the "properties.{collection}:{facet}" convention. Replica host
    # names, for example, live at "assets.alternate:name" across all collections.
    COMMON_AGGREGATIONS = {
        "alternate_name_frequency": "assets.alternate:name",
    }

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
            aggregations = (
                self.COLLECTION_DEFAULT_AGGREGATIONS[collection_id]
                + self.DEFAULT_AGGREGATIONS.copy()
            )
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
        aggregate_request: Optional[EsAggregationExtensionPostRequest] = None,
        aggregations: Optional[str] = None,
        collections: Optional[list] = [],
        collection_id: Optional[str] = None,
        filter_expr: Optional[str] = None,
        filter_lang: Optional[str] = None,
        size: Optional[int] = 10,
        **kwargs,
    ) -> dict[str, Any]:
        request: Request = kwargs.get("request")
        base_url = str(request.base_url)
        path = request.url.path

        search = globus_sdk.SearchQuery()

        if aggregate_request:
            aggregations = aggregate_request.aggregations
            collections = aggregate_request.collections
            size = aggregate_request.size

            # Workaround for optional path param in POST requests
            if "collections" in path and "{collection_id}" not in path:
                collection_id = path.split("/")[2]

        if collection_id:
            if collections:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot specify both 'collection_id' and 'collections' parameters.",
                )
            else:
                collections = [collection_id]

        search = self.database.apply_collections_filter(search, collections)

        # The CQL2 filter must be applied *after* the collection filter. A bare
        # property (e.g. "frequency") resolves to a per-collection namespace
        # ("properties.{collection}:frequency"), and apply_cql2_filter derives
        # that namespace from the collection already present on the search object
        # (see database_logic._extract_collection_ids). Applying it earlier —
        # before apply_collections_filter — leaves the collection invisible to
        # the translation and fails with "require exactly one collection".
        if aggregate_request and aggregate_request.filter_expr:
            try:
                search = self.database.apply_cql2_filter(
                    search, aggregate_request.filter_expr
                )
            except NotImplementedError as e:
                raise HTTPException(status_code=501, detail=str(e))
            except (ValueError, KeyError, IndexError) as e:
                raise HTTPException(
                    status_code=400, detail=f"Malformed CQL2 filter: {e}"
                )

        if aggregations is None or aggregations == []:
            raise HTTPException(
                status_code=400,
                detail="No 'aggregations' found. Use '/aggregations' to return available aggregations",
            )

        links = [{"rel": "root", "type": "application/json", "href": base_url}]
        stac_aggregations = []

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

        # A project facet's index field lives under a per-collection namespace
        # ("properties.{collection}:{facet}"). Resolve that namespace from the
        # request context — the single collection being aggregated — rather than
        # by parsing a project prefix out of the aggregation name. Collection ids
        # such as "CORDEX-CMIP6" contain separators and cannot be recovered by
        # splitting the name on its first separator. This reuses the same
        # namespace helper the CQL2 filter path uses (database_logic), so field
        # resolution has a single source of truth.
        collection_prefix = _collection_property_prefix(collections)

        facet_name_to_aggregation: dict[str, str] = {}
        for aggregation in aggregations:
            if aggregation == "total_count":
                response = await run_in_threadpool(
                    self.client.post_search, settings.search_index_id, search
                )
                return {
                    "type": "AggregationCollection",
                    "aggregations": [
                        {
                            "name": "total_count",
                            "data_type": "integer",
                            "value": response["total"],
                        }
                    ],
                    "links": links,
                }

            if aggregation in self.COMMON_AGGREGATIONS:
                # Non-project-namespaced field (e.g. replica host names).
                field_name = self.COMMON_AGGREGATIONS[aggregation]
                facet_name = aggregation.removesuffix("_frequency")
            else:
                if collection_prefix is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Aggregation '{aggregation}' is collection-namespaced "
                            "and requires exactly one collection to resolve its "
                            "field. Provide a single collection via the "
                            "'collections' field or the "
                            "'/collections/{collection_id}/aggregate' path."
                        ),
                    )
                facet_name = _facet_from_aggregation(aggregation, collection_prefix)
                field_name = f"properties.{collection_prefix}:{facet_name}"

            search.add_facet(
                facet_name,
                field_name=field_name,
                type="terms",
                size=size,
            )
            facet_name_to_aggregation[facet_name] = aggregation

        response = await run_in_threadpool(
            self.client.post_search, settings.search_index_id, search
        )

        if response["facet_results"]:
            for facet in response["facet_results"]:
                stac_buckets = []
                for bucket in facet["buckets"]:
                    stac_bucket = {
                        "key": bucket["value"],
                        "data_type": "frequency_distribution",
                        "frequency": bucket["count"],
                    }
                    stac_buckets.append(stac_bucket)
                stac_aggregation = {
                    "name": facet_name_to_aggregation.get(facet["name"], facet["name"]),
                    "data_type": "frequency_distribution",
                    "buckets": stac_buckets,
                }
                stac_aggregations.append(stac_aggregation)

        return {
            "type": "AggregationCollection",
            "aggregations": stac_aggregations,
            "links": links,
        }


def _facet_from_aggregation(aggregation: str, collection_prefix: str) -> str:
    """Return the bare facet name for a requested frequency aggregation.

    The canonical form is bare (``"activity_id_frequency"`` -> ``"activity_id"``).
    For backwards compatibility a legacy collection/project prefix is also
    accepted and stripped. Both hyphen and underscore spellings of the
    collection are recognised, so for the ``CORDEX-CMIP6`` collection all of
    ``"activity_id_frequency"``, ``"cordex-cmip6_activity_id_frequency"`` and
    ``"cordex_cmip6_activity_id_frequency"`` resolve to ``"activity_id"``.
    """
    facet = aggregation.removesuffix("_frequency")
    legacy_prefixes = [
        f"{collection_prefix.replace('-', '_')}_",  # e.g. "cordex_cmip6_"
        f"{collection_prefix}_",  # e.g. "cordex-cmip6_"
        f"{collection_prefix}-",  # e.g. "cordex-cmip6-"
    ]
    for prefix in legacy_prefixes:
        if facet.startswith(prefix):
            return facet[len(prefix) :]
    return facet
