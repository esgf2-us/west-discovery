# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.1] - 2026-10-01

Hot-fix for compatibility with esgvoc 6.2.0.

### Fixed
- `_build_project` crashed with `AttributeError: 'CatalogProperties' object has
  no attribute 'regex_base_id'` on `GET /collections/{id}`. esgvoc 6.x renamed
  `CatalogProperties.regex_base_id` to `regex_title` (same semantics: the
  dataset id without the version); read `regex_title` for the `base_id_pattern`
  output.

### Changed
- Removed leftover debug `print` statements in `_build_project`.
- Refreshed stale "esgvoc 4.0.0" references in `utility.py` comments and
  docstrings to reflect esgvoc 6.x.
- Updated the `utility` unit-test mock to use `regex_title`.

## [1.0.0] - 2026-09-28

First production release. A STAC API backed by Globus Search, exposing ESGF
climate datasets as STAC Collections and Items with CQL2 filtering, aggregation,
free-text search, and queryables derived from ESGF controlled vocabularies.

### Added
- Queryables endpoint generated from esgvoc, per `collection_id`.
- CQL2 filter support for both bare and collection-qualified property names.
- Accept either `filter` or `filter_expr` on search and aggregate endpoints.
- Support for the `cmip6test` collection.
- `apply_datetime_filter` for start/end temporal extent.

### Changed
- Converted to a multi-stage Dockerfile; dropped Poetry in favor of
  `requirements.txt` / `requirements-dev.txt`.
- Moved all HTTP error handling out of the database layer into the view layer;
  replaced bare assertions and silent failures with proper HTTP errors.
- STAC-standard `numberMatched` / `numberReturned` on ItemCollection responses.
- Transparent qualification of CQL2 property names.
- README rewritten to reflect the current state of the project.

### Fixed
- Resolve collection namespace for CQL2 filters in aggregate.
- Resolve alternate/replica name via `assets.alternate:name`.
- Restore CQL2 field-name namespacing and fail loudly when unresolvable.
- CMIP6Test aggregation namespace and STAC validation.

### Testing
- Unit test coverage expanded to ~95% (200 tests); added error-handling paths;
  queryables tests made independent of the installed esgvoc CV.

[1.0.1]: https://github.com/esgf2-us/west-discovery/releases/tag/v1.0.1
[1.0.0]: https://github.com/esgf2-us/west-discovery/releases/tag/v1.0.0
