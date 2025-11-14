"""Request model for the Aggregation extension."""

from typing import Optional

import attr
from fastapi import Path
from typing_extensions import Annotated

from stac_fastapi.extensions.core.aggregation.request import (
    AggregationExtensionGetRequest,
    AggregationExtensionPostRequest,
)


@attr.s
class GlobusAggregationExtensionGetRequest(
    AggregationExtensionGetRequest
):
    collection_id: Optional[
        Annotated[str, Path(description="Collection ID")]
    ] = attr.ib(default=None)

    size : Optional[int] = attr.ib(default=10)


@attr.s
class GlobusAggregationExtensionPostRequest(
    AggregationExtensionPostRequest
):
    size : Optional[int] = attr.ib(default=10)
