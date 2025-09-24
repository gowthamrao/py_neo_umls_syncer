# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Tests for the downloader module
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import zipfile
import io

from py_neo_umls_syncer.downloader import UMLSDownloader

@pytest.fixture
def downloader(tmp_path):
    """Fixture to create a UMLSDownloader instance with a temporary download directory."""
    return UMLSDownloader(api_key="test_api_key", download_dir=str(tmp_path))

def test_get_release_info_success(downloader, requests_mock):
    """Test that _get_release_info successfully retrieves and finds a release."""
    mock_response = {
        "result": [
            {"name": "2022AA", "downloadUrl": "http://example.com/2022AA.zip", "md5": "abc"},
            {"name": "2022AB", "downloadUrl": "http://example.com/2022AB.zip", "md5": "def"}
        ]
    }
    requests_mock.get(UMLSDownloader.RELEASE_API_URL, json=mock_response)
    release_info = downloader._get_release_info("2022AB")
    assert release_info["name"] == "2022AB"

def test_get_release_info_not_found(downloader, requests_mock):
    """Test that _get_release_info raises ValueError when a release is not found."""
    mock_response = {"result": [{"name": "2022AA"}]}
    requests_mock.get(UMLSDownloader.RELEASE_API_URL, json=mock_response)
    with pytest.raises(ValueError):
        downloader._get_release_info("2023AA")

@patch('py_neo_umls_syncer.downloader.UMLSDownloader._get_release_info')
@patch('py_neo_umls_syncer.downloader.requests.get')
def test_download_and_extract_release(mock_requests_get, mock_get_release_info, downloader, tmp_path):
    """Test the full download and extraction process."""
    # Create a fake zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr("META/test.txt", "test data")
    zip_content = zip_buffer.getvalue()

    # Calculate the checksum of the fake zip file
    import hashlib
    actual_checksum = hashlib.md5(zip_content).hexdigest()

    # Mock release info
    mock_get_release_info.return_value = {
        "name": "2023AA",
        "downloadUrl": "http://example.com/dummy.zip",
        "md5": actual_checksum
    }

    # Mock requests.get for download
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [zip_content]
    mock_response.headers.get.return_value = str(len(zip_content))
    mock_requests_get.return_value.__enter__.return_value = mock_response

    # Run the download process
    extracted_path = downloader.download_and_extract_release("2023AA")

    # Assertions
    assert extracted_path.exists()
    assert extracted_path.is_dir()
    assert (extracted_path / "test.txt").exists()
    assert (extracted_path / "test.txt").read_text() == "test data"

    # Check that the zip file was cleaned up
    zip_filepath = tmp_path / "dummy.zip"
    assert not zip_filepath.exists()
