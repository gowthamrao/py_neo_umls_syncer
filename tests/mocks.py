from unittest.mock import patch

def mock_settings():
    """Mocks key settings for predictable test outcomes."""
    mock_sab_priority = ['SAB1', 'SAB2', 'SAB_NEW']

    # Use patch to temporarily modify the settings object within the modules that use it.
    # This is more robust than patching the source `settings` object directly in some cases.
    patcher1 = patch('pyNeoUmlsSyncer.parser.settings.sab_priority', mock_sab_priority)
    patcher2 = patch('pyNeoUmlsSyncer.config.settings.sab_priority', mock_sab_priority)

    # Allow multiple patches
    patcher1.start()
    patcher2.start()

    # You would add other settings to mock here if needed, e.g., filters.
    # For now, this is sufficient for the parser tests.

    # Return a function to stop the patchers during test teardown if needed,
    # though pytest handles this well for function-scoped fixtures.
    def stop_patches():
        patcher1.stop()
        patcher2.stop()

    return stop_patches
