import pystac_client
import pytest
from globus_sdk import SearchClient, SearchQueryV1

from stac_fastapi.globus_search.database_logic import cql_to_filter

CEDA_STAC = "https://api.stac.esgf.ceda.ac.uk/"
GLOBUS_UUID = "b9b74283-9465-41c3-884a-85188a484bc0"
ITEMS_PER_PAGE = 1000


@pytest.mark.parametrize(
    "cql_filter",
    [
        {
            "op": "=",
            "args": [{"property": "properties.variable_id"}, "rsus"],
        },
        {
            "op": "<>",
            "args": [{"property": "properties.activity_id"}, "CMIP"],
        },
        {
            "op": "or",
            "args": [
                {
                    "args": [{"property": "properties.variable_id"}, "rsus"],
                    "op": "=",
                },
                {
                    "args": [{"property": "properties.variable_id"}, "rsds"],
                    "op": "=",
                },
            ],
        },
        {
            "op": "and",
            "args": [
                {
                    "args": [{"property": "properties.variable_id"}, "rsus"],
                    "op": "=",
                },
                {
                    "args": [{"property": "properties.source_id"}, "EC-Earth3"],
                    "op": "=",
                },
            ],
        },
    ],
)
def test_cql_to_filter(cql_filter):
    """
    Use the input CQL filter to query CEDA STAC and Globus and ensure they
    return the same number of results.
    """
    # STAC via CEDA
    results = pystac_client.Client.open(CEDA_STAC).search(
        collections="cmip6",
        limit=ITEMS_PER_PAGE,
        filter=cql_filter,
    )
    page = next(iter(results.pages()))
    num_stac = page.extra_fields["numMatched"]

    # Convert to GFilter and query Globus
    globus_filter = cql_to_filter(cql_filter)
    results = SearchClient().paginated.post_search(
        GLOBUS_UUID, SearchQueryV1(limit=ITEMS_PER_PAGE, filters=[globus_filter])
    )
    page = next(iter(results))
    num_globus = page.data["total"]
    if num_stac > ITEMS_PER_PAGE:
        raise ValueError(
            f"Test returns more results that in a single page {num_stac=} {ITEMS_PER_PAGE=}"
        )
    assert num_stac == num_globus
