from stac_fastapi.globus_search.utility import _extract_summaries_from_schema


def test_extract_summaries_from_schema(item_schema):
    summaries = _extract_summaries_from_schema(item_schema)

    assert summaries == {
        "activity_id": ["CMIP", "ScenarioMIP"],
        "frequency": ["mon", "day"],
        "variant_label": r"^r\d+i\d+p\d+f\d+$",
        "member_id": [
            {"pattern": r"^r\d+i\d+p\d+f\d+$"},
            {"pattern": r"^r\d+i\d+p\d+$"},
        ],
    }
