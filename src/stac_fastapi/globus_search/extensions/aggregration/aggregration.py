"""Request model for the Aggregation extension."""

from typing import Optional

import attr
from fastapi import Path
from stac_fastapi.extensions.core.aggregation.request import (
    AggregationExtensionGetRequest,
    AggregationExtensionPostRequest,
)
from stac_fastapi.extensions.core.filter.request import (
    FilterExtensionGetRequest,
    FilterExtensionPostRequest,
)
from typing_extensions import Annotated


@attr.s
class GlobusAggregationExtensionGetRequest(
    AggregationExtensionGetRequest, FilterExtensionGetRequest
):
    collection_id: Optional[Annotated[str, Path(description="Collection ID")]] = (
        attr.ib(default=None)
    )

    size: Optional[int] = attr.ib(default=10)


@attr.s
class GlobusAggregationExtensionPostRequest(
    AggregationExtensionPostRequest, FilterExtensionPostRequest
):
    size: Optional[int] = 10
