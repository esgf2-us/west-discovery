import pytest

from stac_fastapi.globus_search import utility


@pytest.fixture(autouse=True)
def fake_summaries(monkeypatch):
    monkeypatch.setattr(
        utility,
        "_build_project_summaries",
        lambda: [
            {"id": "cmip6", "title": "CMIP6"},
            {"id": "cmip6plus", "title": "CMIP6Plus"},
            {"id": "cordex-cmip6", "title": "CORDEX-CMIP6"},
            {"id": "obs4ref", "title": "obs4REF"},
        ],
    )
    utility._collection_id_map.cache_clear()
    yield
    utility._collection_id_map.cache_clear()


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("cmip6", "CMIP6"),
        ("CMIP6", "CMIP6"),
        ("Cmip6Plus", "CMIP6Plus"),
        ("cordex-cmip6", "CORDEX-CMIP6"),
        ("OBS4REF", "obs4REF"),
        ("cmip6test", "CMIP6Test"),
        ("unknown", "unknown"),
    ],
)
def test_canonical_collection_id(given, expected):
    assert utility.canonical_collection_id(given) == expected


def test_apply_collections_filter_canonicalizes():
    # Imported here: database_logic builds a Globus client at import time.
    from stac_fastapi.globus_search.database_logic import DatabaseLogic

    search = DatabaseLogic.make_search()
    DatabaseLogic.apply_collections_filter(search, ["cmip6", "CMIP6Plus"])
    assert search["filters"][0]["values"] == ["CMIP6", "CMIP6Plus"]
