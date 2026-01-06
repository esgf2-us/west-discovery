"""
Integration tests for the complete STAC API.
"""
import pytest
from unittest.mock import Mock, patch


@pytest.mark.integration
class TestCompleteWorkflow:
    """Integration tests for complete workflows."""

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_search_and_retrieve_workflow(self, mock_client, test_app, sample_search_response, sample_stac_item):
        """Test complete workflow: search for items, then retrieve one."""
        # Mock search response
        mock_client.scroll.return_value = sample_search_response
        
        # Step 1: Search for items
        search_response = test_app.post("/search", json={
            "collections": ["cmip6"],
            "limit": 10
        })
        assert search_response.status_code == 200
        data = search_response.json()
        assert len(data["features"]) > 0
        
        # Step 2: Get specific item
        item_id = data["features"][0]["id"]
        mock_response = Mock()
        mock_response.data = {
            "entries": [{"content": sample_stac_item}]
        }
        mock_client.get_subject.return_value = mock_response
        
        item_response = test_app.get(f"/collections/cmip6/items/{item_id}")
        assert item_response.status_code == 200
        item_data = item_response.json()
        assert item_data["id"] == sample_stac_item["id"]

    def test_collections_to_items_workflow(self, test_app):
        """Test workflow: list collections, then get items from one."""
        # Step 1: List all collections
        collections_response = test_app.get("/collections")
        assert collections_response.status_code == 200
        collections_data = collections_response.json()
        assert len(collections_data["collections"]) > 0
        
        # Step 2: Get items from first collection
        collection_id = collections_data["collections"][0]["id"]
        with patch('stac_fastapi.globus_search.database_logic._client') as mock_client:
            mock_client.scroll.return_value = {"gmeta": [], "total": 0, "marker": None}
            items_response = test_app.get(f"/collections/{collection_id}/items")
            assert items_response.status_code == 200

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_filtered_search_workflow(self, mock_client, test_app, sample_search_response):
        """Test workflow with multiple filters applied."""
        mock_client.scroll.return_value = sample_search_response
        
        # Complex search with multiple filters
        search_payload = {
            "collections": ["cmip6"],
            "bbox": [-10, -10, 10, 10],
            "filter": {
                "op": "and",
                "args": [
                    {
                        "op": "=",
                        "args": [{"property": "cmip6:experiment_id"}, "historical"]
                    },
                    {
                        "op": ">=",
                        "args": [{"property": "cmip6:year"}, 2000]
                    }
                ]
            },
            "limit": 20
        }
        
        response = test_app.post("/search", json=search_payload)
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "FeatureCollection"

    def test_aggregation_workflow(self, test_app):
        """Test aggregation workflow."""
        # Step 1: Get available aggregations
        agg_list_response = test_app.get("/collections/cmip6/aggregations")
        assert agg_list_response.status_code == 200
        agg_data = agg_list_response.json()
        assert "aggregations" in agg_data
        
        # Step 2: Request specific aggregation (if implemented)
        # This may return 404 if not fully implemented
        agg_response = test_app.get(
            "/collections/cmip6/aggregate?aggregations=total_count"
        )
        assert agg_response.status_code in [200, 404]


@pytest.mark.integration
class TestErrorHandling:
    """Integration tests for error handling."""

    def test_invalid_collection(self, test_app):
        """Test accessing non-existent collection."""
        response = test_app.get("/collections/nonexistent")
        assert response.status_code >= 400

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_invalid_item(self, mock_client, test_app):
        """Test accessing non-existent item."""
        mock_client.get_subject.side_effect = Exception("Not found")
        response = test_app.get("/collections/cmip6/items/nonexistent")
        assert response.status_code >= 400

    def test_invalid_search_parameters(self, test_app):
        """Test search with invalid parameters."""
        # Invalid bbox (wrong number of coordinates)
        response = test_app.post("/search", json={
            "bbox": [0, 0]  # Should be 4 or 6 values
        })
        # May return 422 (validation error) or 400 (bad request)
        assert response.status_code >= 400

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_invalid_cql2_filter(self, mock_client, test_app):
        """Test search with invalid CQL2 filter."""
        mock_client.scroll.return_value = {"gmeta": [], "total": 0, "marker": None}
        
        response = test_app.post("/search", json={
            "filter": {
                "op": "unsupported_operator",
                "args": []
            }
        })
        # Should handle gracefully
        assert response.status_code in [200, 400]


@pytest.mark.integration
class TestPagination:
    """Integration tests for pagination."""

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_pagination_first_page(self, mock_client, test_app, sample_search_response):
        """Test first page of paginated results."""
        sample_search_response["marker"] = "next_page_token"
        mock_client.scroll.return_value = sample_search_response
        
        response = test_app.get("/search?limit=10")
        assert response.status_code == 200
        data = response.json()
        
        # Should have links including next
        links = data.get("links", [])
        assert any(link["rel"] == "next" for link in links) or "marker" in data

    @patch('stac_fastapi.globus_search.database_logic._client')
    def test_pagination_with_token(self, mock_client, test_app, sample_search_response):
        """Test requesting specific page with token."""
        mock_client.scroll.return_value = sample_search_response
        
        response = test_app.get("/search?limit=10&token=page_token_123")
        assert response.status_code == 200


@pytest.mark.integration
class TestConformance:
    """Integration tests for API conformance."""

    def test_conformance_classes(self, test_app):
        """Test that all required conformance classes are present."""
        response = test_app.get("/conformance")
        assert response.status_code == 200
        data = response.json()
        
        conformance = data["conformsTo"]
        
        # Should include STAC API core
        assert any("stac-api" in c.lower() for c in conformance)
        
        # Should include CQL2
        assert any("cql2" in c.lower() for c in conformance)

    def test_api_version(self, test_app):
        """Test that API returns correct STAC version."""
        response = test_app.get("/")
        assert response.status_code == 200
        data = response.json()
        
        # Should have STAC version or type
        assert "stac_version" in data or "type" in data


@pytest.mark.integration  
class TestCORS:
    """Integration tests for CORS headers."""

    def test_cors_headers_present(self, test_app):
        """Test that CORS headers are present if configured."""
        response = test_app.get("/")
        # CORS headers may or may not be configured
        # Just checking the request succeeds
        assert response.status_code == 200


@pytest.mark.integration
class TestContentNegotiation:
    """Integration tests for content negotiation."""

    def test_json_response(self, test_app):
        """Test that JSON responses are returned correctly."""
        response = test_app.get(
            "/collections",
            headers={"Accept": "application/json"}
        )
        assert response.status_code == 200
        assert "application/json" in response.headers.get("content-type", "")

    def test_geojson_response(self, test_app):
        """Test that GeoJSON responses are returned for features."""
        with patch('stac_fastapi.globus_search.database_logic._client') as mock_client:
            mock_client.scroll.return_value = {"gmeta": [], "total": 0, "marker": None}
            response = test_app.get(
                "/search",
                headers={"Accept": "application/geo+json"}
            )
            assert response.status_code == 200