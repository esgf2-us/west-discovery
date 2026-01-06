"""
Tests for database logic and CQL2 filter conversion.
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi import HTTPException

from src.stac_fastapi.globus_search.database_logic import (
    DatabaseLogic,
    cql_to_filter,
    cql_translate_fieldname
)


class TestCqlTranslateFieldname:
    """Tests for CQL field name translation."""

    def test_translate_id(self):
        assert cql_translate_fieldname("id") == "id"

    def test_translate_collection(self):
        assert cql_translate_fieldname("collection") == "collection"

    def test_translate_geometry(self):
        assert cql_translate_fieldname("geometry") == "geometry"

    def test_translate_property(self):
        assert cql_translate_fieldname("temperature") == "properties.temperature"

    def test_translate_cmip6_property(self):
        assert cql_translate_fieldname("cmip6:experiment_id") == "properties.cmip6:experiment_id"


class TestCqlToFilter:
    """Tests for CQL2 to Globus Search filter conversion."""

    def test_empty_filter(self):
        """Test that empty CQL query returns empty filter."""
        result = cql_to_filter({})
        assert result == {}

    def test_not_operator(self):
        """Test NOT operator conversion."""
        cql = {
            "op": "not",
            "args": [{
                "op": "=",
                "args": [{"property": "id"}, "test-123"]
            }]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "not"
        assert "filter" in result

    def test_double_not_simplification(self):
        """Test that not(not(x)) simplifies to x."""
        inner_filter = {"type": "match_any", "field_name": "id", "values": ["test"]}
        cql = {
            "op": "not",
            "args": [{"type": "not", "filter": inner_filter}]
        }
        result = cql_to_filter(cql)
        # Double negation should return the inner filter
        assert result == inner_filter

    def test_and_operator(self):
        """Test AND operator conversion."""
        cql = {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, "test-1"]},
                {"op": "=", "args": [{"property": "collection"}, "cmip6"]}
            ]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "and"
        assert len(result["filters"]) == 2

    def test_or_operator(self):
        """Test OR operator conversion."""
        cql = {
            "op": "or",
            "args": [
                {"op": "=", "args": [{"property": "id"}, "test-1"]},
                {"op": "=", "args": [{"property": "id"}, "test-2"]}
            ]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "or"
        assert len(result["filters"]) == 2

    def test_equals_operator(self):
        """Test equals (=) operator conversion."""
        cql = {
            "op": "=",
            "args": [{"property": "collection"}, "cmip6"]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "match_any"
        assert result["field_name"] == "collection"
        assert result["values"] == ["cmip6"]

    def test_not_equals_operator(self):
        """Test not equals (<>) operator conversion."""
        cql = {
            "op": "<>",
            "args": [{"property": "status"}, "archived"]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "not"
        assert result["filter"]["type"] == "match_any"

    def test_lte_operator(self):
        """Test less than or equal (<=) operator conversion."""
        cql = {
            "op": "<=",
            "args": [{"property": "temperature"}, 30]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "range"
        assert result["field_name"] == "properties.temperature"
        assert result["values"][0]["from"] == "*"
        assert result["values"][0]["to"] == 30

    def test_gte_operator(self):
        """Test greater than or equal (>=) operator conversion."""
        cql = {
            "op": ">=",
            "args": [{"property": "temperature"}, 10]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "range"
        assert result["field_name"] == "properties.temperature"
        assert result["values"][0]["from"] == 10
        assert result["values"][0]["to"] == "*"

    def test_is_null_operator(self):
        """Test isNull operator conversion."""
        cql = {
            "op": "isNull",
            "args": [{"property": "optional_field"}]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "not"
        assert result["filter"]["type"] == "exists"
        assert result["filter"]["field_name"] == "properties.optional_field"

    def test_in_operator(self):
        """Test IN operator conversion."""
        cql = {
            "op": "in",
            "args": [{"property": "status"}, ["active", "pending", "completed"]]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "match_any"
        assert result["field_name"] == "status"
        assert result["values"] == ["active", "pending", "completed"]

    def test_s_intersects_operator(self):
        """Test s_intersects spatial operator conversion."""
        geometry = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        }
        cql = {
            "op": "s_intersects",
            "args": [{"property": "geometry"}, geometry]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "geo_shape"
        assert result["relation"] == "intersects"
        assert result["shape"] == geometry

    def test_s_within_operator(self):
        """Test s_within spatial operator conversion."""
        geometry = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        }
        cql = {
            "op": "s_within",
            "args": [{"property": "geometry"}, geometry]
        }
        result = cql_to_filter(cql)
        assert result["type"] == "geo_shape"
        assert result["relation"] == "within"
        assert result["shape"] == geometry

    def test_unsupported_lt_gt_operators(self):
        """Test that < and > operators raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="'>' and '<' filters are not supported yet"):
            cql_to_filter({"op": "<", "args": [{"property": "temp"}, 20]})

        with pytest.raises(NotImplementedError, match="'>' and '<' filters are not supported yet"):
            cql_to_filter({"op": ">", "args": [{"property": "temp"}, 20]})

    def test_unsupported_like_operator(self):
        """Test that LIKE operator raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="'like' filter is not supported yet"):
            cql_to_filter({"op": "like", "args": [{"property": "name"}, "%test%"]})

    def test_unsupported_array_operators(self):
        """Test that array operators raise ValueError."""
        for op in ["a_equals", "a_contains", "a_contained_by", "a_overlaps"]:
            with pytest.raises(ValueError, match="is not supported"):
                cql_to_filter({"op": op, "args": []})

    def test_unrecognized_operator(self):
        """Test that unrecognized operators raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Unrecgonized operator"):
            cql_to_filter({"op": "unknown_op", "args": []})


class TestDatabaseLogic:
    """Tests for DatabaseLogic class."""

    @pytest.fixture
    def db_logic(self):
        return DatabaseLogic()

    @pytest.mark.asyncio
    async def test_find_collection_cmip6(self, db_logic):
        """Test finding CMIP6 collection by ID (case insensitive)."""
        collection = await db_logic.find_collection("cmip6")
        assert collection["id"] == "CMIP6"
        assert collection["type"] == "Collection"

    @pytest.mark.asyncio
    async def test_find_collection_uppercase(self, db_logic):
        """Test finding collection with uppercase ID."""
        collection = await db_logic.find_collection("CMIP6")
        assert collection["id"] == "CMIP6"
        assert collection["type"] == "Collection"

    @pytest.mark.asyncio
    async def test_find_collection_obs4mips(self, db_logic):
        """Test finding obs4MIPs collection."""
        collection = await db_logic.find_collection("obs4mips")
        assert collection["id"] == "obs4MIPs"
        assert collection["type"] == "Collection"

    @pytest.mark.asyncio
    async def test_find_nonexistent_collection(self, db_logic):
        """Test that finding nonexistent collection raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            await db_logic.find_collection("nonexistent")
        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_get_all_collections(self, db_logic, mock_request):
        """Test getting all collections."""
        collections, token = await db_logic.get_all_collections(
            token=None, 
            limit=10, 
            request=mock_request
        )
        assert isinstance(collections, list)
        assert len(collections) == 2  # CMIP6 and obs4MIPs
        assert all(c["type"] == "Collection" for c in collections)

    def test_make_search(self, db_logic):
        """Test creating a search query."""
        search = db_logic.make_search()
        assert search is not None
        # Should be a SearchScrollQuery instance
        import globus_sdk
        assert isinstance(search, globus_sdk.SearchScrollQuery)

    def test_apply_ids_filter(self, db_logic):
        """Test applying IDs filter to search."""
        search = db_logic.make_search()
        item_ids = ["item-1", "item-2", "item-3"]
        result = db_logic.apply_ids_filter(search, item_ids)
        assert result is not None
        # Verify filter was added
        filters = result.get("filters", [])
        assert any(f.get("type") == "match_any" and f.get("field_name") == "id" for f in filters)

    def test_apply_collections_filter(self, db_logic):
        """Test applying collections filter to search."""
        search = db_logic.make_search()
        collection_ids = ["cmip6", "cordex"]
        result = db_logic.apply_collections_filter(search, collection_ids)
        assert result is not None
        # Verify filter was added
        filters = result.get("filters", [])
        assert any(f.get("type") == "match_any" and f.get("field_name") == "collection" for f in filters)

    def test_apply_bbox_filter(self, db_logic):
        """Test applying bounding box filter to search."""
        search = db_logic.make_search()
        bbox = [-10, -10, 10, 10]
        result = db_logic.apply_bbox_filter(search, bbox)
        assert "filters" in result
        filters = result["filters"]
        assert any(f["type"] == "geo_bounding_box" for f in filters)
        # Verify bbox structure
        bbox_filter = next(f for f in filters if f["type"] == "geo_bounding_box")
        assert bbox_filter["field_name"] == "geometry"
        assert "top_left" in bbox_filter
        assert "bottom_right" in bbox_filter

    def test_apply_intersects_filter(self, db_logic):
        """Test applying intersects filter (placeholder)."""
        search = db_logic.make_search()
        shape = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        }
        result = db_logic.apply_intersects_filter(search, shape)
        # Currently returns search unchanged
        assert result is not None

    def test_apply_cql2_filter(self, db_logic):
        """Test applying CQL2 filter to search."""
        search = db_logic.make_search()
        cql_filter = {
            "op": "=",
            "args": [{"property": "collection"}, "cmip6"]
        }
        result = db_logic.apply_cql2_filter(search, cql_filter)
        assert "filters" in result
        assert len(result["filters"]) > 0

    def test_apply_cql2_filter_none(self, db_logic):
        """Test applying None CQL2 filter (should do nothing)."""
        search = db_logic.make_search()
        result = db_logic.apply_cql2_filter(search, None)
        assert result == search

    @pytest.mark.asyncio
    async def test_execute_search(self, db_logic, mock_globus_client, sample_search_response):
        """Test executing a search query."""
        mock_globus_client.scroll.return_value = sample_search_response
        
        search = db_logic.make_search()
        items, total, marker = await db_logic.execute_search(
            search=search,
            limit=10,
            token=None,
            sort=None,
            collection_ids=["cmip6"]
        )
        
        assert isinstance(items, list)
        assert total == sample_search_response["total"]
        assert marker == sample_search_response["marker"]
        mock_globus_client.scroll.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_search_with_token(self, db_logic, mock_globus_client, sample_search_response):
        """Test executing search with pagination token."""
        mock_globus_client.scroll.return_value = sample_search_response
        
        search = db_logic.make_search()
        search.set_query("*")
        search.set_limit(5)
        
        items, total, marker = await db_logic.execute_search(
            search=search,
            limit=5,
            token="test_token_123",
            sort=None,
            collection_ids=None
        )
        
        assert isinstance(items, list)
        # Verify token was set
        call_args = mock_globus_client.scroll.call_args
        assert call_args is not None

    @pytest.mark.asyncio
    async def test_get_one_item(self, db_logic, mock_globus_client, sample_search_doc):
        """Test getting a single item by ID."""
        mock_response = Mock()
        mock_response.data = sample_search_doc
        mock_globus_client.get_subject.return_value = mock_response

        item = await db_logic.get_one_item("CMIP6", "test-item-123")

        assert item["id"] == "test-item-123"
        assert item["type"] == "Feature"
        mock_globus_client.get_subject.assert_called_once()
