from types import SimpleNamespace

import pytest

from stac_fastapi.globus_search import utility
from stac_fastapi.globus_search.utility import (DrsType,
                                                _extract_summaries_from_schema,
                                                get_project, list_projects)


@pytest.fixture(autouse=True)
def clear_project_caches():
    utility._build_project.cache_clear()
    utility._build_project_summaries.cache_clear()
    utility._build_projects.cache_clear()
    yield
    utility._build_project.cache_clear()
    utility._build_project_summaries.cache_clear()
    utility._build_projects.cache_clear()


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


def _project_specs(*, catalog_specs=True, drs_specs=True):
    catalog = (
        SimpleNamespace(
            catalog_properties=SimpleNamespace(
                extensions=[
                    SimpleNamespace(name="cmip6", version="1.0.0"),
                    SimpleNamespace(name="projection", version="2.0.0"),
                ],
                url_template=(
                    "https://schemas.example.org/{extension_name}/"
                    "{extension_version}/schema.json"
                ),
                regex_id=r"^dataset-id$",
                regex_base_id=r"^base-id$",
            )
        )
        if catalog_specs
        else None
    )
    drs = (
        {
            DrsType.DATASET_ID: SimpleNamespace(
                separator=".",
                parts=[
                    SimpleNamespace(source_collection="activity_id", is_required=True),
                    SimpleNamespace(source_collection="source_id", is_required=False),
                    SimpleNamespace(
                        source_collection="experiment_id", is_required=True
                    ),
                ],
            )
        }
        if drs_specs
        else None
    )
    return SimpleNamespace(
        project_id="cmip6",
        drs_name="CMIP6",
        description="CMIP6 project",
        version="abc123",
        catalog_specs=catalog,
        drs_specs=drs,
    )


def test_get_project_builds_stac_collection(monkeypatch, item_schema):
    monkeypatch.setattr(utility.ev, "get_project", lambda project_id: _project_specs())
    monkeypatch.setattr(
        utility,
        "generate_json_schema",
        lambda project_id: item_schema,
    )

    collection = get_project("CMIP6")

    assert collection == {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "cmip6",
        "title": "CMIP6",
        "description": "CMIP6 project",
        "version": "abc123",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {
                "interval": [["0850-01-01T00:00:00Z", "2300-12-31T00:00:00Z"]]
            },
        },
        "summaries": {
            "activity_id": ["CMIP", "ScenarioMIP"],
            "frequency": ["mon", "day"],
            "variant_label": r"^r\d+i\d+p\d+f\d+$",
            "member_id": [
                {"pattern": r"^r\d+i\d+p\d+f\d+$"},
                {"pattern": r"^r\d+i\d+p\d+$"},
            ],
        },
        "item_assets": {
            "data": {
                "type": "application/netcdf",
                "roles": ["data"],
                "title": "NetCDF data file",
            }
        },
        "links": [
            {"rel": "root", "href": "./collection.json", "type": "application/json"},
            {
                "rel": "describedby",
                "href": "https://github.com/WCRP-CMIP/CMIP6_CVs",
                "type": "text/html",
                "title": (
                    "CMIP6 CV \u2014 dataset_id template: "
                    "{activity_id}.{experiment_id}"
                ),
            },
            {
                "rel": "describedby",
                "href": "https://schemas.example.org/cmip6/1.0.0/schema.json",
                "type": "application/schema+json",
                "title": "STAC cmip6 extension schema 1.0.0",
            },
            {
                "rel": "describedby",
                "href": "https://schemas.example.org/projection/2.0.0/schema.json",
                "type": "application/schema+json",
                "title": "STAC projection extension schema 2.0.0",
            },
        ],
        "dataset_id_pattern": r"^dataset-id$",
        "base_id_pattern": r"^base-id$",
    }


def test_get_project_lowercases_project_id_for_esgvoc_and_schema(monkeypatch):
    calls = []

    def fake_get_project(project_id):
        calls.append(("get_project", project_id))
        return _project_specs(drs_specs=False)

    def fake_generate_json_schema(project_id):
        calls.append(("generate_json_schema", project_id))
        return {"definitions": {"item_fields": {"properties": {}}}}

    monkeypatch.setattr(utility.ev, "get_project", fake_get_project)
    monkeypatch.setattr(utility, "generate_json_schema", fake_generate_json_schema)

    collection = get_project("CMIP6")

    assert calls == [
        ("get_project", "cmip6"),
        ("generate_json_schema", "cmip6"),
    ]
    assert collection["summaries"] == {}
    assert len(collection["links"]) == 3


def test_get_project_raises_for_unknown_project(monkeypatch):
    monkeypatch.setattr(utility.ev, "get_project", lambda project_id: None)

    with pytest.raises(ValueError, match="Project 'missing' not found"):
        get_project("missing")


def test_get_project_raises_when_catalog_specs_are_missing(monkeypatch):
    monkeypatch.setattr(
        utility.ev,
        "get_project",
        lambda project_id: _project_specs(catalog_specs=False),
    )

    with pytest.raises(ValueError, match="has no catalog_specs"):
        get_project("cmip6")


def test_list_projects_returns_collections_and_skips_invalid_projects(
    monkeypatch, caplog
):
    monkeypatch.setattr(utility.ev, "get_all_projects", lambda: ["cmip6", "bad"])

    def fake_get_project(project_id):
        if project_id == "bad":
            raise ValueError("not configured")
        return {"id": project_id}

    monkeypatch.setattr(utility, "_build_project", fake_get_project)

    results, token = list_projects()

    assert results == [{"id": "cmip6"}]
    assert token is None
    assert "Skipping 'bad': not configured" in caplog.text


def _project_summary_specs(project_id, drs_name, *, has_catalog_specs=True):
    return SimpleNamespace(
        project_id=project_id,
        drs_name=drs_name,
        catalog_specs=object() if has_catalog_specs else None,
    )


def test_list_project_summaries_returns_id_and_title_for_each_project(monkeypatch):
    monkeypatch.setattr(utility.ev, "get_all_projects", lambda: ["cmip6", "obs4mips"])

    def fake_get_project(project_id):
        return {
            "cmip6": _project_summary_specs("cmip6", "CMIP6"),
            "obs4mips": _project_summary_specs("obs4mips", "obs4MIPs"),
        }[project_id]

    monkeypatch.setattr(utility.ev, "get_project", fake_get_project)

    result = utility.list_project_summaries()

    assert result == [
        {"id": "cmip6", "title": "CMIP6"},
        {"id": "obs4mips", "title": "obs4MIPs"},
    ]


def test_list_project_summaries_skips_projects_not_found_in_esgvoc(monkeypatch, caplog):
    monkeypatch.setattr(utility.ev, "get_all_projects", lambda: ["cmip6", "unknown"])

    def fake_get_project(project_id):
        if project_id == "unknown":
            return None
        return _project_summary_specs("cmip6", "CMIP6")

    monkeypatch.setattr(utility.ev, "get_project", fake_get_project)

    result = utility.list_project_summaries()

    assert result == [{"id": "cmip6", "title": "CMIP6"}]
    assert "Skipping 'unknown': project not found in esgvoc" in caplog.text


def test_list_project_summaries_skips_projects_without_catalog_specs(
    monkeypatch, caplog
):
    monkeypatch.setattr(utility.ev, "get_all_projects", lambda: ["cmip6", "incomplete"])

    def fake_get_project(project_id):
        if project_id == "incomplete":
            return _project_summary_specs(
                "incomplete", "Incomplete", has_catalog_specs=False
            )
        return _project_summary_specs("cmip6", "CMIP6")

    monkeypatch.setattr(utility.ev, "get_project", fake_get_project)

    result = utility.list_project_summaries()

    assert result == [{"id": "cmip6", "title": "CMIP6"}]
    assert "Skipping 'incomplete': project has no catalog_specs" in caplog.text
