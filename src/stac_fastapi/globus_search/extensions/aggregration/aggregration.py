"""Request model for the Aggregation extension."""

from typing import Any, Dict, Optional

import attr
from fastapi import Path
from pydantic import AliasChoices, ConfigDict, Field
from stac_fastapi.extensions.core.aggregation.request import (
    AggregationExtensionGetRequest,
    AggregationExtensionPostRequest,
)
from stac_fastapi.extensions.core.filter.request import (
    FilterExtensionGetRequest,
    FilterExtensionPostRequest,
)
from typing_extensions import Annotated


# Default number of buckets returned per frequency aggregation. The upstream
# default of 10 silently truncates high-cardinality facets such as
# institution_id or source_id.
DEFAULT_FACET_SIZE = 100


@attr.s
class GlobusAggregationExtensionGetRequest(
    AggregationExtensionGetRequest, FilterExtensionGetRequest
):
    model_config = ConfigDict(populate_by_name=True)

    collection_id: Optional[Annotated[str, Path(description="Collection ID")]] = (
        attr.ib(default=None)
    )
    size: Optional[int] = attr.ib(default=DEFAULT_FACET_SIZE)


class GlobusAggregationExtensionPostRequest(
    AggregationExtensionPostRequest, FilterExtensionPostRequest
):
    # Accept the CQL2 filter under either "filter" (STAC spec) or "filter_expr".
    # See GlobusFilterExtensionPostRequest for why the alias is on the field
    # rather than model_config.
    filter_expr: Optional[Dict[str, Any]] = Field(
        default=None,
        validation_alias=AliasChoices("filter", "filter_expr"),
        serialization_alias="filter",
        description="A CQL2 filter expression. Accepts 'filter' or 'filter_expr'.",
    )

    size: Optional[int] = Field(
        DEFAULT_FACET_SIZE,
        description="The number of aggregation results to return.",
    )
