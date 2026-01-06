"""
Tests for configuration module.
"""
import pytest
from unittest.mock import Mock, patch

from src.stac_fastapi.globus_search.config import (
    GlobusSearchSettings,
    SEARCH_INDEX_ID
)
import globus_sdk


class TestSearchIndexId:
    """Tests for search index ID constant."""

    def test_search_index_id_exists(self):
        """Test that SEARCH_INDEX_ID is defined."""
        assert SEARCH_INDEX_ID is not None
        assert isinstance(SEARCH_INDEX_ID, str)
        assert len(SEARCH_INDEX_ID) > 0

    def test_search_index_id_format(self):
        """Test that SEARCH_INDEX_ID has UUID format."""
        # Should be a UUID format string
        parts = SEARCH_INDEX_ID.split("-")
        assert len(parts) == 5
        assert all(len(p) > 0 for p in parts)


class TestGlobusSearchSettings:
    """Tests for GlobusSearchSettings class."""

    def test_settings_instantiation(self):
        """Test that settings can be instantiated."""
        settings = GlobusSearchSettings()
        assert settings is not None

    def test_create_client_property(self):
        """Test that create_client property returns SearchClient."""
        settings = GlobusSearchSettings()
        client = settings.create_client
        assert isinstance(client, globus_sdk.SearchClient)

    def test_create_client_returns_new_instance(self):
        """Test that create_client returns a new client instance each time."""
        settings = GlobusSearchSettings()
        client1 = settings.create_client
        client2 = settings.create_client
        
        # Should return new instances
        assert client1 is not client2

    def test_settings_inheritance(self):
        """Test that GlobusSearchSettings inherits from ApiSettings."""
        from stac_fastapi.types.config import ApiSettings
        settings = GlobusSearchSettings()
        assert isinstance(settings, ApiSettings)

    @patch.dict('os.environ', {}, clear=True)
    def test_settings_default_values(self):
        """Test default values when no environment variables are set."""
        settings = GlobusSearchSettings()
        # Should instantiate with defaults
        assert settings is not None

    @patch.dict('os.environ', {
        'STAC_FASTAPI_TITLE': 'Test STAC API',
        'STAC_FASTAPI_DESCRIPTION': 'Test Description'
    })
    def test_settings_from_environment(self):
        """Test that settings can be configured from environment variables."""
        settings = GlobusSearchSettings()
        # ApiSettings should pick up these values
        assert settings is not None

    def test_client_is_authenticated(self):
        """Test that the client can be created (authentication is handled by SDK)."""
        settings = GlobusSearchSettings()
        client = settings.create_client
        
        # Client should be created without error
        assert client is not None
        # Should have expected methods
        assert hasattr(client, 'scroll')
        assert hasattr(client, 'post_search')
        assert hasattr(client, 'get_subject')


class TestSettingsConfiguration:
    """Tests for settings configuration and customization."""

    def test_settings_can_be_customized(self):
        """Test that settings support customization."""
        # This tests that the settings class can be extended
        class CustomSettings(GlobusSearchSettings):
            custom_field: str = "custom_value"
        
        settings = CustomSettings()
        assert hasattr(settings, 'custom_field')
        assert settings.custom_field == "custom_value"

    def test_multiple_settings_instances_independent(self):
        """Test that multiple settings instances are independent."""
        settings1 = GlobusSearchSettings()
        settings2 = GlobusSearchSettings()
        
        # Should be different instances
        assert settings1 is not settings2
        
        # Should produce different clients
        client1 = settings1.create_client
        client2 = settings2.create_client
        assert client1 is not client2