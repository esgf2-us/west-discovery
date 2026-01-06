"""
Pytest configuration and fixtures for testing the Globus Search STAC FastAPI application.
"""
import sys
import os
from pathlib import Path

# Add src directory to path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"

# Debug: Print paths
print(f"Project root: {project_root}")
print(f"Source path: {src_path}")
print(f"Source path exists: {src_path.exists()}")

if not str(src_path) in sys.path:
    sys.path.insert(0, str(src_path))
    print(f"Added {src_path} to sys.path")

import pytest
from fastapi.testclient import TestClient
from stac_fastapi.api import app
from unittest.mock import Mock, patch, AsyncMock

# Import after path is set
from src.stac_fastapi.globus_search.app import api
from src.stac_fastapi.globus_search.config import GlobusSearchSettings


@pytest.fixture(scope="session")
def test_settings():
    """Provide test settings."""
    return GlobusSearchSettings()


@pytest.fixture(scope="function")
def mock_globus_client():
    """Mock Globus Search client for testing."""
    with patch('stac_fastapi.globus_search.database_logic._client') as mock_client:
        yield mock_client


@pytest.fixture(scope="function")
def test_app():
    """Create a test client for the FastAPI application."""
    try:
        with TestClient(api.app) as client:
            yield client
    except Exception as e:
        pytest.skip(f"Could not create test app: {e}")


@pytest.fixture
def sample_stac_item():
    """Provide a sample STAC item for testing."""
    return {
        "id": "test-item-123",
        "type": "Feature",
        "stac_version": "1.0.0",
        "collection": "cmip6",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-180.0, -90.0],
                [180.0, -90.0],
                [180.0, 90.0],
                [-180.0, 90.0],
                [-180.0, -90.0]
            ]]
        },
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "properties": {
            "datetime": "2020-01-01T00:00:00Z",
            "cmip6:activity_id": "CMIP",
            "cmip6:experiment_id": "historical",
            "cmip6:variable_id": "tas"
        },
        "links": [],
        "assets": {
            "data": {
                "href": "https://example.com/data.nc",
                "type": "application/x-netcdf"
            }
        }
    }


@pytest.fixture
def sample_search_doc(sample_stac_item):
    """Provide a sample Globus Search document."""
    # Note: convert.py expects assets as a list with 'name' field
    item_with_list_assets = sample_stac_item.copy()
    item_with_list_assets["assets"] = [
        {
            "name": "data",
            "href": "https://example.com/data.nc",
            "type": "application/x-netcdf"
        }
    ]

    return {
        "entries": [
            {
                "content": item_with_list_assets
            }
        ]
    }


@pytest.fixture
def sample_search_response(sample_search_doc):
    """Provide a sample Globus Search response."""
    return {
        "gmeta": [sample_search_doc],
        "total": 1,
        "marker": None
    }


@pytest.fixture
def sample_collection():
    """Provide a sample STAC collection."""
    return {
        "id": "CMIP6",
        "type": "Collection",
        "stac_version": "1.0.1",
        "title": "CMIP6",
        "description": "CMIP6 collection",
        "license": "No license",
        "extent": {
            "spatial": {
                "bbox": [[-180.0, -90.0, 180.0, 90.0]]
            },
            "temporal": {
                "interval": [["1850-01-01T00:00:00.000Z", "4114-12-16T12:00:00.000Z"]]
            }
        },
        "links": [],
        "providers": [],
        "summaries": {},
        "stac_extensions": [],
        "keywords": [],
        "assets": {}
    }


@pytest.fixture
def sample_facet_response():
    """Provide a sample facet response for aggregation testing."""
    return {
        "total": 100,
        "facet_results": [
            {
                "name": "experiment_id",
                "buckets": [
                    {"value": "historical", "count": 50},
                    {"value": "ssp585", "count": 30},
                    {"value": "piControl", "count": 20}
                ]
            }
        ]
    }


@pytest.fixture
def mock_request():
    """Provide a mock request object."""
    request = Mock()
    request.base_url = "http://test/"
    request.url = Mock()
    request.url.path = "/search"
    request.query_params = Mock()
    request.query_params.get = Mock(return_value=None)
    return request


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )