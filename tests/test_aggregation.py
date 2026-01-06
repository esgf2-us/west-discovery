"""
Tests for aggregation extension functionality.
"""
import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException

from stac_fastapi.core.session import Session
from src.stac_fastapi.globus_search.extensions.aggregration.client import (
    GlobusSearchAggregationClient
)
from src.stac_fastapi.globus_search.database_logic import DatabaseLogic
from src.stac_fastapi.globus_search.config import GlobusSearchSettings


class TestGlobusSearchAggregationClient:
    """Tests for Globus Search aggregation client."""

    @pytest.fixture
    def aggregation_client(self):
        """Create an aggregation client instance."""
        database = DatabaseLogic()
        settings = GlobusSearchSettings()
        session = Session.create_from_settings(settings)
        return GlobusSearchAggregationClient(
            database=database,
            session=session,
            settings=settings
        )

    @pytest.mark.asyncio
    async def test_get_aggregations_root(self, aggregation_client, mock_request):
        """Test getting aggregations at root level."""
        result = await aggregation_client.get_aggregations(
            collection_id=None,
            request=mock_request
        )
        
        assert result["type"] == "AggregationCollection"
        assert "aggregations" in result
        assert "links" in result
        # Root level should only have default aggregations
        assert len(result["aggregations"]) == len(
            aggregation_client.DEFAULT_AGGREGATIONS
        )

    @pytest.mark.asyncio
    async def test_get_aggregations_collection(self, aggregation_client, mock_request):
        """Test getting aggregations for a collection."""
        result = await aggregation_client.get_aggregations(
            collection_id="cmip6",
            request=mock_request
        )
        
        assert result["type"] == "AggregationCollection"
        assert "aggregations" in result
        # Collection level should have CMIP6 + default aggregations
        expected_count = (
            len(aggregation_client.CMIP6_DEFAULT_AGGREGATIONS) +
            len(aggregation_client.DEFAULT_AGGREGATIONS)
        )
        assert len(result["aggregations"]) == expected_count

    @pytest.mark.asyncio
    async def test_get_aggregations_links(self, aggregation_client, mock_request):
        """Test that aggregations response includes proper links."""
        result = await aggregation_client.get_aggregations(
            collection_id="cmip6",
            request=mock_request
        )
        
        links = result["links"]
        assert any(link["rel"] == "root" for link in links)
        assert any(link["rel"] == "self" for link in links)
        assert any(link["rel"] == "collection" for link in links)

    @pytest.mark.asyncio
    async def test_aggregate_total_count(self, aggregation_client, mock_request):
        """Test aggregate with total_count aggregation."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_response = {"total": 1234, "facet_results": []}
            mock_client.post_search.return_value = mock_response
            
            result = await aggregation_client.aggregate(
                aggregations=["total_count"],
                collection_id="cmip6",
                request=mock_request
            )
            
            assert result["type"] == "AggregationCollection"
            assert len(result["aggregations"]) == 1
            assert result["aggregations"][0]["name"] == "total_count"
            assert result["aggregations"][0]["data_type"] == "integer"
            assert result["aggregations"][0]["value"] == 1234

    @pytest.mark.asyncio
    async def test_aggregate_facets(self, aggregation_client, mock_request):
        """Test aggregate with facet aggregations."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_response = {
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
            mock_client.post_search.return_value = mock_response
            
            result = await aggregation_client.aggregate(
                aggregations=["cmip6_experiment_id_frequency"],
                collection_id="cmip6",
                request=mock_request
            )
            
            assert result["type"] == "AggregationCollection"
            assert len(result["aggregations"]) == 1
            
            agg = result["aggregations"][0]
            assert agg["name"] == "cmip6_experiment_id_frequency"
            assert agg["data_type"] == "frequency_distribution"
            assert "buckets" in agg
            assert len(agg["buckets"]) == 3

    @pytest.mark.asyncio
    async def test_aggregate_bucket_structure(self, aggregation_client, mock_request):
        """Test that aggregation buckets have correct structure."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_response = {
                "total": 100,
                "facet_results": [
                    {
                        "name": "variable_id",
                        "buckets": [
                            {"value": "tas", "count": 40},
                            {"value": "pr", "count": 35}
                        ]
                    }
                ]
            }
            mock_client.post_search.return_value = mock_response
            
            result = await aggregation_client.aggregate(
                aggregations=["cmip6_variable_id_frequency"],
                collection_id="cmip6",
                request=mock_request
            )
            
            buckets = result["aggregations"][0]["buckets"]
            for bucket in buckets:
                assert "key" in bucket
                assert "data_type" in bucket
                assert "frequency" in bucket
                assert bucket["data_type"] == "frequency_distribution"

    @pytest.mark.asyncio
    async def test_aggregate_empty_results(self, aggregation_client, mock_request):
        """Test aggregate with no facet results."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_client.post_search.return_value = {
                "total": 0,
                "facet_results": []
            }
            
            result = await aggregation_client.aggregate(
                aggregations=["cmip6_experiment_id_frequency"],
                collection_id="cmip6",
                request=mock_request
            )
            
            assert result["type"] == "AggregationCollection"
            assert "aggregations" in result

    @pytest.mark.asyncio
    async def test_aggregate_no_aggregations_error(self, aggregation_client, mock_request):
        """Test that empty aggregations list raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            await aggregation_client.aggregate(
                aggregations=[],
                collection_id="cmip6",
                request=mock_request
            )
        
        assert exc_info.value.status_code == 400
        assert "aggregations" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_aggregate_with_post_request(self, aggregation_client, mock_request):
        """Test aggregate with POST request body."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_client.post_search.return_value = {"total": 100, "facet_results": []}
            
            # Mock aggregate request
            aggregate_request = Mock()
            aggregate_request.filter_expr = None
            aggregate_request.aggregations = ["total_count"]
            aggregate_request.collections = ["cmip6"]
            aggregate_request.size = 10
            
            result = await aggregation_client.aggregate(
                aggregate_request=aggregate_request,
                request=mock_request
            )
            
            assert result["type"] == "AggregationCollection"

    @pytest.mark.asyncio
    async def test_aggregate_collection_id_conflict_error(self, aggregation_client, mock_request):
        """Test that specifying both collection_id and collections raises error."""
        with pytest.raises(HTTPException) as exc_info:
            await aggregation_client.aggregate(
                aggregations=["total_count"],
                collections=["cmip6"],
                collection_id="cmip6",
                request=mock_request
            )
        
        assert exc_info.value.status_code == 400
        assert "collection_id" in exc_info.value.detail.lower()
        assert "collections" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_aggregate_with_cql2_filter(self, aggregation_client, mock_request):
        """Test aggregate with CQL2 filter."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_client.post_search.return_value = {"total": 50, "facet_results": []}
            
            aggregate_request = Mock()
            aggregate_request.filter_expr = {
                "op": "=",
                "args": [{"property": "cmip6:experiment_id"}, "historical"]
            }
            aggregate_request.aggregations = ["total_count"]
            aggregate_request.collections = None
            aggregate_request.size = 10
            
            result = await aggregation_client.aggregate(
                aggregate_request=aggregate_request,
                collection_id="cmip6",
                request=mock_request
            )
            
            assert result["type"] == "AggregationCollection"

    def test_cmip6_aggregations_list(self, aggregation_client):
        """Test that CMIP6 aggregations list contains expected fields."""
        expected_fields = [
            "activity_id", "cf_standard_name", "experiment_id",
            "frequency", "grid", "institution", "variable_id"
        ]
        
        agg_names = [
            agg["name"] for agg in aggregation_client.CMIP6_DEFAULT_AGGREGATIONS
        ]
        
        for field in expected_fields:
            expected_name = f"cmip6_{field}_frequency"
            assert any(expected_name in name for name in agg_names)

    def test_default_aggregations_structure(self, aggregation_client):
        """Test that default aggregations have correct structure."""
        for agg in aggregation_client.DEFAULT_AGGREGATIONS:
            assert "name" in agg
            assert "data_type" in agg

    @pytest.mark.asyncio
    async def test_aggregate_with_size_parameter(self, aggregation_client, mock_request):
        """Test aggregate with custom size parameter."""
        with patch.object(aggregation_client, 'client') as mock_client:
            mock_response = {
                "total": 100,
                "facet_results": [
                    {
                        "name": "experiment_id",
                        "buckets": [{"value": f"exp{i}", "count": i} for i in range(20)]
                    }
                ]
            }
            mock_client.post_search.return_value = mock_response
            
            result = await aggregation_client.aggregate(
                aggregations=["cmip6_experiment_id_frequency"],
                collection_id="cmip6",
                size=20,
                request=mock_request
            )
            
            # Verify size was used
            assert len(result["aggregations"][0]["buckets"]) == 20