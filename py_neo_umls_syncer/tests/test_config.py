# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import os
import json
from py_neo_umls_syncer.config import get_settings, Settings

def test_settings_load_from_env(monkeypatch):
    """
    Tests that the Settings class correctly loads values from environment variables.
    """
    # Arrange: Set environment variables
    monkeypatch.setenv("UMLS_API_KEY", "test-api-key")
    monkeypatch.setenv("NEO4J_URI", "bolt://test:7687")
    # For complex types like lists/sets, pydantic-settings expects a JSON-encoded string
    sab_filter_json = json.dumps(["SAB1", "SAB2"])
    monkeypatch.setenv("SAB_FILTER", sab_filter_json)

    # Clear the cache for the settings factory to force re-reading from env
    get_settings.cache_clear()

    # Act: Get the settings object
    settings = get_settings()

    # Assert: Check that the settings have been loaded correctly
    assert settings.UMLS_API_KEY == "test-api-key"
    assert settings.NEO4J_URI == "bolt://test:7687"
    assert settings.SAB_FILTER == {"SAB1", "SAB2"}
    assert settings.APOC_BATCH_SIZE == 10000

    # Clean up
    get_settings.cache_clear()
