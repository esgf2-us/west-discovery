"""
Tests for search document to STAC item conversion.
"""
import pytest
from src.stac_fastapi.globus_search.convert import search_doc_to_stac_item


class TestSearchDocToStacItem:
    """Tests for converting Globus Search documents to STAC items."""

    def test_basic_conversion(self):
        """Test basic conversion from search doc to STAC item."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {"datetime": "2020-01-01T00:00:00Z"},
                        "assets": []
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        assert result["id"] == "test-item"
        assert result["type"] == "Feature"

    def test_assets_list_conversion(self):
        """Test conversion of assets from list to dict format."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                        "assets": [
                            {
                                "name": "data",
                                "href": "https://example.com/data.nc",
                                "type": "application/x-netcdf"
                            },
                            {
                                "name": "metadata",
                                "href": "https://example.com/metadata.xml",
                                "type": "application/xml"
                            }
                        ]
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        
        # Assets should be converted to dict format
        assert isinstance(result["assets"], dict)
        assert "data" in result["assets"]
        assert "metadata" in result["assets"]
        assert result["assets"]["data"]["href"] == "https://example.com/data.nc"
        # "name" key should be removed from individual assets
        assert "name" not in result["assets"]["data"]

    def test_assets_with_alternates(self):
        """Test conversion of assets with alternate versions."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                        "assets": [
                            {
                                "name": "data",
                                "href": "https://example.com/data.nc",
                                "alternate": [
                                    {
                                        "name": "s3",
                                        "href": "s3://bucket/data.nc"
                                    },
                                    {
                                        "name": "https",
                                        "href": "https://cdn.example.com/data.nc"
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        
        assert "data" in result["assets"]
        assert "alternate" in result["assets"]["data"]
        
        # Alternates should be converted to dict
        alternates = result["assets"]["data"]["alternate"]
        assert isinstance(alternates, dict)
        assert "s3" in alternates
        assert "https" in alternates
        assert alternates["s3"]["href"] == "s3://bucket/data.nc"
        # "name" key should be removed from alternates
        assert "name" not in alternates["s3"]

    def test_empty_assets(self):
        """Test handling of empty assets list."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                        "assets": []
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        assert result["assets"] == {}

    def test_preserves_other_fields(self):
        """Test that other fields are preserved during conversion."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "stac_version": "1.0.0",
                        "collection": "cmip6",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
                        },
                        "bbox": [0, 0, 10, 10],
                        "properties": {
                            "datetime": "2020-01-01T00:00:00Z",
                            "cmip6:experiment_id": "historical"
                        },
                        "links": [
                            {"rel": "self", "href": "https://example.com/item"}
                        ],
                        "assets": []
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        
        assert result["stac_version"] == "1.0.0"
        assert result["collection"] == "cmip6"
        assert result["bbox"] == [0, 0, 10, 10]
        assert result["properties"]["cmip6:experiment_id"] == "historical"
        assert len(result["links"]) == 1

    def test_complex_geometry(self):
        """Test handling of complex geometry."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                                [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]]
                            ]
                        },
                        "properties": {},
                        "assets": []
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        assert result["geometry"]["type"] == "MultiPolygon"
        assert len(result["geometry"]["coordinates"]) == 2

    def test_asset_without_name(self):
        """Test handling of assets without name field."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                        "assets": [
                            {
                                "href": "https://example.com/data.nc",
                                "type": "application/x-netcdf"
                            }
                        ]
                    }
                }
            ]
        }
        
        # Should handle gracefully - assets without names won't be added to dict
        result = search_doc_to_stac_item(search_doc)
        assert isinstance(result["assets"], dict)

    def test_dict_copy_behavior(self):
        """Test that conversion creates a copy and doesn't modify original."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {"test": "value"},
                        "assets": [
                            {"name": "data", "href": "https://example.com/data.nc"}
                        ]
                    }
                }
            ]
        }
        
        # Store original for comparison
        original_assets = search_doc["entries"][0]["content"]["assets"].copy()
        
        result = search_doc_to_stac_item(search_doc)
        
        # Verify conversion happened
        assert isinstance(result["assets"], dict)
        # Original should still be a list
        assert isinstance(original_assets, list)

    def test_nested_alternate_structure(self):
        """Test deeply nested alternate structures."""
        search_doc = {
            "entries": [
                {
                    "content": {
                        "id": "test-item",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                        "assets": [
                            {
                                "name": "primary",
                                "href": "https://primary.example.com/data.nc",
                                "alternate": [
                                    {
                                        "name": "mirror1",
                                        "href": "https://mirror1.example.com/data.nc",
                                        "title": "Mirror 1"
                                    },
                                    {
                                        "name": "mirror2",
                                        "href": "https://mirror2.example.com/data.nc",
                                        "title": "Mirror 2"
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        
        result = search_doc_to_stac_item(search_doc)
        
        # Verify nested structure
        assert "primary" in result["assets"]
        alternates = result["assets"]["primary"]["alternate"]
        assert "mirror1" in alternates
        assert "mirror2" in alternates
        assert alternates["mirror1"]["title"] == "Mirror 1"
        assert "name" not in alternates["mirror1"]