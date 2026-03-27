import json
import esgvoc.api.projects as ev
from esgvoc.api.project_specs import DrsType
from esgvoc.apps.jsg.json_schema_generator import generate_json_schema


def _extract_summaries_from_schema(schema: dict) -> dict:
    """
    Extract STAC summaries from the JSON Schema produced by generate_json_schema().

    The schema follows a STAC extension pattern where item properties live at:
        definitions -> item_fields -> properties

    (Top-level `properties` only contains `stac_extensions` — not what we want.)
    """
    summaries = {}

    item_properties = (
        schema
        .get("definitions", {})
        .get("item_fields", {})
        .get("properties", {})
    )

    for field_name, field_schema in item_properties.items():
        # Array fields (e.g. activity_id, realm, source_type) nest their
        # constraints inside "items"
        if field_schema.get("type") == "array":
            inner = field_schema.get("items", {})
        else:
            inner = field_schema

        if "enum" in inner:
            summaries[field_name] = inner["enum"]
        elif "pattern" in inner:
            summaries[field_name] = inner["pattern"]
        elif "anyOf" in inner:
            # Composite terms with pattern components
            summaries[field_name] = inner["anyOf"]
        # else: only "type" present (source_collection: null) → skip

    return summaries


def build_stac_collection(project_id: str = "cmip6") -> dict:
    """
    Generate a STAC-compliant Collection for an ESGF project.
    Summaries and field validation constraints are derived entirely from
    generate_json_schema(), with no hardcoded field mappings.
    """
    # ── 1. Project-level metadata ─────────────────────────────────────────────
    specs = ev.get_project(project_id)
    if specs is None:
        raise ValueError(f"Project '{project_id}' not found in esgvoc")

    # ── 2. Generate item schema — also the source of summaries ───────────────
    item_schema = generate_json_schema(project_id)

    # ── 3. Extract summaries from schema properties ───────────────────────────
    summaries = _extract_summaries_from_schema(item_schema)

    # ── 4. Links ──────────────────────────────────────────────────────────────
    links = [{"rel": "root", "href": "./collection.json", "type": "application/json"}]

    # DRS dataset_id template
    if specs.drs_specs and DrsType.DATASET_ID in specs.drs_specs:
        drs = specs.drs_specs[DrsType.DATASET_ID]
        sep = drs.separator
        template = sep.join(
            f"{{{p.source_collection}}}" for p in drs.parts if p.is_required
        )
        links.append({
            "rel": "describedby",
            "href": "https://github.com/WCRP-CMIP/CMIP6_CVs",
            "type": "text/html",
            "title": f"CMIP6 CV — dataset_id template: {template}"
        })

    # STAC extension schema links from catalog_properties
    if specs.catalog_specs:
        cat = specs.catalog_specs.catalog_properties
        for ext in cat.extensions:
            url = cat.url_template.format(
                extension_name=ext.name,
                extension_version=ext.version
            )
            links.append({
                "rel": "describedby",
                "href": url,
                "type": "application/schema+json",
                "title": f"STAC {ext.name} extension schema {ext.version}"
            })

        # The item schema itself as a describedby link
        links.append({
            "rel": "describedby",
            "href": f"https://your-stac-api/{project_id}-item-schema.json",
            "type": "application/schema+json",
            "title": f"{specs.drs_name} item property JSON Schema (generated from esgvoc CVs)"
        })

    # ── 5. Assemble the STAC Collection ──────────────────────────────────────
    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": project_id,
        "title": specs.drs_name,
        "description": specs.description,
        "version": specs.version,
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [["0850-01-01T00:00:00Z", "2300-12-31T00:00:00Z"]]}
        },
        "summaries": summaries,
        "links": links,
    }
