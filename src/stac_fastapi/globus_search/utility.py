import json
from copy import deepcopy
from functools import lru_cache

import esgvoc.api.projects as ev
from esgvoc.api.project_specs import DrsType
from esgvoc.apps.jsg.json_schema_generator import generate_json_schema


def _extract_summaries_from_schema(schema: dict) -> dict:
    """
    Extract STAC summaries from the JSON Schema produced by generate_json_schema().

    In 4.0.0 the schema follows a STAC extension pattern where item properties live at:
        definitions -> item_fields -> properties

    The top-level `properties` only contains `stac_extensions` — not what we want.

    Field types:
      - plain/composite enum fields  → list of valid values (possibly nested under "items")
      - pattern fields               → regex string for validation
      - null-source fields           → only have "type", no constraint values → skip
    """
    summaries = {}

    # ── Correct path: definitions → item_fields → properties ─────────────────
    item_properties = (
        schema.get("definitions", {}).get("item_fields", {}).get("properties", {})
    )

    for field_name, field_schema in item_properties.items():
        # Array fields (e.g. activity_id, realm, source_type) wrap their
        # constraints inside "items"; scalar fields have them at the top level.
        if field_schema.get("type") == "array":
            inner = field_schema.get("items", {})
        else:
            inner = field_schema

        if "enum" in inner:
            summaries[field_name] = inner["enum"]
        elif "pattern" in inner:
            summaries[field_name] = inner["pattern"]
        elif "anyOf" in inner:
            # Composite terms expressed as a union of patterns
            summaries[field_name] = inner["anyOf"]
        # else: source_collection was null → only "type" present → skip

    return summaries


@lru_cache(maxsize=None)
def _build_project(project_id: str = "cmip6") -> dict:
    """
    Generate a STAC-compliant Collection for an ESGF project.
    Summaries are derived entirely from generate_json_schema() via catalog_specs —
    no hardcoded field mappings required.

    Compatible with esgvoc >= 4.0.0.
    """
    # ── 1. Project-level metadata ─────────────────────────────────────────────
    specs = ev.get_project(project_id.lower())
    if specs is None:
        raise ValueError(f"Project '{project_id}' not found in esgvoc")

    if specs.catalog_specs is None:
        raise ValueError(
            f"Project '{project_id}' has no catalog_specs — cannot derive summaries"
        )

    # In 4.0.0 ProjectSpecs carries an explicit project_id field
    canonical_id = specs.project_id  # e.g. "cmip6"
    drs_name = specs.drs_name  # e.g. "CMIP6"
    cat = specs.catalog_specs.catalog_properties

    # ── 2. Generate JSON Schema → extract summaries ───────────────────────────
    item_schema = generate_json_schema(project_id.lower())
    summaries = _extract_summaries_from_schema(item_schema)

    # ── 3. Links ──────────────────────────────────────────────────────────────
    links = [{"rel": "root", "href": "./collection.json", "type": "application/json"}]

    # DRS dataset_id template from drs_specs
    if specs.drs_specs and DrsType.DATASET_ID in specs.drs_specs:
        drs = specs.drs_specs[DrsType.DATASET_ID]
        template = drs.separator.join(
            f"{{{p.source_collection}}}" for p in drs.parts if p.is_required
        )
        links.append(
            {
                "rel": "describedby",
                "href": f"https://github.com/WCRP-CMIP/CMIP6_CVs",
                "type": "text/html",
                "title": f"{drs_name} CV — dataset_id template: {template}",
            }
        )

    # STAC extension schema links from catalog_properties
    for ext in cat.extensions:
        url = cat.url_template.format(
            extension_name=ext.name, extension_version=ext.version
        )
        links.append(
            {
                "rel": "describedby",
                "href": url,
                "type": "application/schema+json",
                "title": f"STAC {ext.name} extension schema {ext.version}",
            }
        )

    # ── 4. item_assets: file-level schema now available via file_properties ───
    # 4.0.0 exposes catalog_specs.file_properties for asset field constraints.
    # We declare a canonical "data" asset here; a full asset schema would be
    # built from file_properties in the same way summaries are from dataset_properties.
    item_assets = {
        "data": {
            "type": "application/netcdf",
            "roles": ["data"],
            "title": "NetCDF data file",
        }
    }

    # ── 5. Assemble the STAC Collection ──────────────────────────────────────
    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": canonical_id,
        "title": drs_name,
        "description": specs.description,
        "version": specs.version,  # CV git hash
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {
                "interval": [["0850-01-01T00:00:00Z", "2300-12-31T00:00:00Z"]]
            },
        },
        "summaries": summaries,
        "item_assets": item_assets,
        "links": links,
        # Dataset and base ID regex patterns surfaced from catalog_properties (new in 4.0.0)
        "dataset_id_pattern": cat.regex_id,
        "base_id_pattern": cat.regex_base_id,
    }


def get_project(project_id: str = "cmip6") -> dict:
    return deepcopy(_build_project(project_id))


@lru_cache(maxsize=1)
def _build_projects() -> tuple[list[dict], None]:
    """
    Build the full project list once per process.

    Collection responses are mutated downstream when links are normalized, so public
    accessors return deep copies instead of the cached objects.
    """
    results = []

    for project_id in ev.get_all_projects():
        try:
            results.append(_build_project(project_id))
        except ValueError as e:
            # Skip projects with missing catalog_specs or not yet fully configured
            print(f"Skipping '{project_id}': {e}")

    return results, None


def list_projects() -> tuple[list[dict], None]:
    """
    Return a full STAC Collection document for every project available
    in the local esgvoc installation.

    Returns the same structure as build_stac_collection() for each project,
    i.e. [{cmip6 collection...}, {obs4mips collection...}, ...]
    """
    return deepcopy(_build_projects())


if __name__ == "__main__":
    collection = get_project("cmip6")
    print(json.dumps(collection, indent=2))
