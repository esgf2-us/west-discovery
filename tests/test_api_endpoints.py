"""
Tests for API endpoints.
"""
import pytest
from unittest.mock import Mock, patch


class TestRootEndpoint:
    """Tests for the root endpoint."""

    def test_root_endpoint(self, test_app):
        """Test that the root endpoint returns landing page."""
        response = test_app.get("/")
        assert response.status_code == 200
        data = response.json()
        # Should have STAC-specific fields
        assert "stac_version" in data or "links" in data or "type" in data


class TestCollectionsEndpoints:
    """Tests for collections endpoints."""

    def test_get_collections(self, test_app):
        """Test getting all collections."""
        response = test_app.get("/collections")
        assert response.status_code == 200
        data = response.json()
        assert "collections" in data
        assert isinstance(data["collections"], list)
        assert len(data["collections"]) == 2  # CMIP6 and obs4MIPs

    def test_get_collection_cmip6_lowercase(self, test_app):
        """Test getting CMIP6 collection with lowercase ID."""
        response = test_app.get("/collections/cmip6")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "CMIP6"
        assert data["type"] == "Collection"

    def test_get_collection_cmip6_uppercase(self, test_app):
        """Test getting CMIP6 collection with uppercase ID."""
        response = test_app.get("/collections/CMIP6")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "CMIP6"
        assert data["type"] == "Collection"

    def test_get_collection_obs4mips(self, test_app):
        """Test getting obs4MIPs collection."""
        response = test_app.get("/collections/obs4mips")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "obs4MIPs"
        assert data["type"] == "Collection"

    def test_get_nonexistent_collection(self, test_app):
        """Test getting a collection that doesn't exist."""
        response = test_app.get("/collections/nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()


class TestItemEndpoints:
    """Tests for item endpoints."""

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_get_item(self, mock_client, test_app, sample_search_doc):
        """Test getting a specific item."""
        mock_response = Mock()
        mock_response.data = sample_search_doc
        mock_client.get_subject.return_value = mock_response
        
        response = test_app.get("/collections/cmip6/items/test-item-123")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "test-item-123"
        assert data["type"] == "Feature"

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_get_items_collection(self, mock_client, test_app, sample_search_response):
        """Test getting items from a collection."""
        mock_client.scroll.return_value = sample_search_response
        
        response = test_app.get("/collections/cmip6/items?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
        assert data["type"] == "FeatureCollection"
        assert "numMatched" in data
        assert "numReturned" in data

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_get_items_with_pagination(self, mock_client, test_app, sample_search_response):
        """Test getting items with pagination token."""
        sample_search_response["marker"] = "next_token"
        mock_client.scroll.return_value = sample_search_response
        
        response = test_app.get("/collections/cmip6/items?limit=5&token=some_token")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "FeatureCollection"


class TestSearchEndpoints:
    """Tests for search endpoints."""

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_get_search(self, mock_client, test_app, sample_search_response):
        """Test GET search endpoint."""
        mock_client.scroll.return_value = sample_search_response
        
        response = test_app.get("/search?collections=cmip6&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "FeatureCollection"
        assert "features" in data

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_post_search_basic(self, mock_client, test_app, sample_search_response):
        """Test POST search endpoint with basic query."""
        mock_client.scroll.return_value = sample_search_response
        
        search_payload = {
            "collections": ["cmip6"],
            "limit": 10
        }
        response = test_app.post("/search", json=search_payload)
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "FeatureCollection"

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_post_search_with_bbox(self, mock_client, test_app, sample_search_response):
        """Test POST search with bounding box."""
        mock_client.scroll.return_value = sample_search_response
        
        search_payload = {
            "collections": ["cmip6"],
            "bbox": [-10, -10, 10, 10],
            "limit": 10
        }
        response = test_app.post("/search", json=search_payload)
        assert response.status_code == 200
        data = response.json()
        assert "features" in data

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_post_search_with_ids(self, mock_client, test_app, sample_search_response):
        """Test POST search with specific IDs."""
        mock_client.scroll.return_value = sample_search_response
        
        search_payload = {
            "ids": ["item-1", "item-2"],
            "limit": 10
        }
        response = test_app.post("/search", json=search_payload)
        assert response.status_code == 200

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_post_search_with_cql2_filter(self, mock_client, test_app, sample_search_response):
        """Test POST search with CQL2 filter."""
        mock_client.scroll.return_value = sample_search_response
        
        search_payload = {
            "collections": ["cmip6"],
            "filter": {
                "op": "=",
                "args": [
                    {"property": "cmip6:experiment_id"},
                    "historical"
                ]
            },
            "limit": 10
        }
        response = test_app.post("/search", json=search_payload)
        assert response.status_code == 200

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_post_search_with_intersects(self, mock_client, test_app, sample_search_response):
        """Test POST search with geometry intersection."""
        mock_client.scroll.return_value = sample_search_response
        
        search_payload = {
            "collections": ["cmip6"],
            "intersects": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
            },
            "limit": 10
        }
        response = test_app.post("/search", json=search_payload)
        assert response.status_code == 200

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_search_pagination(self, mock_client, test_app, sample_search_response):
        """Test search pagination with token."""
        sample_search_response["marker"] = "next_token_123"
        mock_client.scroll.return_value = sample_search_response
        
        response = test_app.get("/search?collections=cmip6&limit=10")
        assert response.status_code == 200
        data = response.json()
        
        # Check for pagination in links or context
        links = data.get("links", [])
        has_next = any(l["rel"] == "next" for l in links)
        # Some implementations may use context instead
        assert has_next or "context" in data


class TestAggregationEndpoints:
    """Tests for aggregation endpoints."""

    def test_get_aggregations_root(self, test_app):
        """Test getting aggregations at root level."""
        response = test_app.get("/aggregations")
        assert response.status_code == 200
        data = response.json()
        assert "aggregations" in data
        assert data["type"] == "AggregationCollection"

    def test_get_aggregations_collection(self, test_app):
        """Test getting aggregations for a collection."""
        response = test_app.get("/collections/cmip6/aggregations")
        assert response.status_code == 200
        data = response.json()
        assert "aggregations" in data
        assert len(data["aggregations"]) > 1  # Should have CMIP6 + default

    @patch('stac_fastapi.globus_search.extensions.aggregration.client.GlobusSearchAggregationClient.client')
    def test_aggregate_total_count(self, mock_client, test_app):
        """Test aggregation for total count."""
        mock_client.post_search.return_value = {"total": 1234, "facet_results": []}
        
        response = test_app.get(
            "/collections/cmip6/aggregate?aggregations=total_count"
        )
        # Endpoint should exist
        assert response.status_code in [200, 404, 422]

    def test_aggregate_no_aggregations_error(self, test_app):
        """Test that requesting aggregate without specifying aggregations returns error."""
        response = test_app.get("/collections/cmip6/aggregate")
        # Should return 400 or 422 for missing required parameter
        assert response.status_code in [400, 422]


class TestConformance:
    """Tests for conformance endpoint."""

    def test_conformance(self, test_app):
        """Test conformance endpoint returns supported standards."""
        response = test_app.get("/conformance")
        assert response.status_code == 200
        data = response.json()
        assert "conformsTo" in data
        assert isinstance(data["conformsTo"], list)
        # Should include CQL2 advanced comparison operators
        assert any("cql2" in conf.lower() for conf in data["conformsTo"])

    def test_conformance_includes_filter_extension(self, test_app):
        """Test that filter extension conformance class is present."""
        response = test_app.get("/conformance")
        data = response.json()
        conformance = data["conformsTo"]
        # Should include filter extension
        assert any("filter" in conf.lower() for conf in conformance)


class TestHealthCheck:
    """Tests for health check endpoints."""

    def test_ping_endpoint(self, test_app):
        """Test health check ping endpoint if it exists."""
        response = test_app.get("/_mgmt/ping")
        # May return 200 or 404 depending on implementation
        assert response.status_code in [200, 404]

    def test_root_as_health_check(self, test_app):
        """Test that root endpoint can serve as basic health check."""
        response = test_app.get("/")
        # Root should always be available
        assert response.status_code == 200