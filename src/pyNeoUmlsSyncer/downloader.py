"""
UMLS Distribution Downloader.

This module handles the download, verification, and extraction of the
UMLS Rich Release Format (RRF) data distribution.
"""
import logging
import requests
import zipfile
from pathlib import Path
from tqdm import tqdm
import hashlib
from typing import Optional

from .config import settings

logger = logging.getLogger(__name__)

class UmlsDownloader:
    """
    Manages the acquisition of UMLS data files.
    """
    def __init__(self):
        self.api_key = settings.umls_api_key
        self.version = settings.umls_version
        self.data_dir = Path(settings.data_dir)
        self.version_dir = self.data_dir / self.version
        self.download_target_path = self.data_dir / f"umls-{self.version}.zip"

    def _get_download_url(self) -> str:
        """
        Constructs the download URL for the UMLS distribution.

        NOTE: This is a MOCK implementation. The actual UMLS download process
        is more complex, involving API authentication with the UTS to get
        a ticket, which is then used to access a download link. This mock
        uses a static, public URL to a sample ZIP file for demonstration.
        """
        logger.warning(
            "Using a mock download URL. This will not download the actual UMLS release."
        )
        # A stable URL to a small sample zip file for testing purposes.
        return "https://www.learningcontainer.com/wp-content/uploads/2020/05/sample-zip-file.zip"

    def _get_expected_checksum(self) -> str:
        """
        Retrieves the expected SHA256 checksum for the download.

        NOTE: This is a MOCK implementation. In a real scenario, the checksum
        would be provided by the UMLS on the download page. Here, it is
        hardcoded to match the sample ZIP file from the mock URL.
        """
        logger.warning("Using a hardcoded mock checksum for verification.")
        return "2a327063895471a41857216859368b0229417f842512a84055531804153a5624"

    def download_release(self) -> bool:
        """Downloads the UMLS release if it doesn't already exist."""
        if self.download_target_path.exists():
            logger.info(f"UMLS zip already exists at '{self.download_target_path}'. Skipping download.")
            return True

        url = self._get_download_url()
        logger.info(f"Downloading UMLS distribution for {self.version} from mock URL...")

        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                with open(self.download_target_path, 'wb') as f, tqdm(
                    total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {self.version}"
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        pbar.update(len(chunk))
            logger.info(f"Successfully downloaded to '{self.download_target_path}'.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download UMLS distribution: {e}")
            if self.download_target_path.exists():
                self.download_target_path.unlink() # Clean up partial download
            return False

    def verify_checksum(self) -> bool:
        """Verifies the checksum of the downloaded file against the expected value."""
        if not self.download_target_path.exists():
            logger.error(f"Cannot verify checksum, file not found: '{self.download_target_path}'")
            return False

        expected_checksum = self._get_expected_checksum()

        logger.info(f"Verifying checksum for '{self.download_target_path}'...")
        sha256 = hashlib.sha256()
        with open(self.download_target_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        actual_checksum = sha256.hexdigest()

        if actual_checksum == expected_checksum:
            logger.info("Checksum verification successful.")
            return True
        else:
            logger.error(f"Checksum mismatch for '{self.download_target_path}'!")
            logger.error(f"  - Expected: {expected_checksum}")
            logger.error(f"  - Actual:   {actual_checksum}")
            logger.error("Deleting corrupted file.")
            self.download_target_path.unlink()
            return False

    def extract_release(self) -> bool:
        """Extracts the downloaded zip file."""
        # A simple check if the directory exists and is not empty.
        if self.version_dir.exists() and any(self.version_dir.iterdir()):
             logger.info(f"Target directory '{self.version_dir}' is not empty. Skipping extraction.")
             return True

        if not self.download_target_path.exists():
            logger.error(f"Cannot extract, zip file not found: '{self.download_target_path}'")
            return False

        logger.info(f"Extracting '{self.download_target_path}' to '{self.version_dir}'...")
        self.version_dir.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(self.download_target_path, 'r') as zip_ref:
                for file in tqdm(zip_ref.infolist(), desc=f"Extracting {self.version}"):
                    zip_ref.extract(file, self.version_dir)
            logger.info("Extraction complete.")
            return True
        except zipfile.BadZipFile:
            logger.error(f"Error: Not a valid zip file or corrupted: '{self.download_target_path}'")
            return False

    def get_release_path(self) -> Path:
        """
        Returns the path where the RRF files are located.

        The actual UMLS distribution has a nested structure like `2024AA/2024AA/META`.
        This method will search for the META directory to be robust.
        """
        search_path = self.version_dir
        meta_paths = list(search_path.rglob('META'))
        if meta_paths:
            # Assuming the first META directory found is the correct one
            rrf_path = meta_paths[0]
            logger.info(f"Found UMLS META directory at: {rrf_path}")
            return rrf_path

        logger.warning(
            f"Could not find a 'META' subdirectory in '{search_path}'. "
            "Returning the base extraction directory. The parser may fail."
        )
        return self.version_dir

    def run(self) -> Optional[Path]:
        """
        Orchestrates the entire download, verification, and extraction process.

        Returns:
            The path to the extracted RRF files ('META' directory) if successful,
            otherwise None.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)

        if not self.download_release():
            return None

        if not self.verify_checksum():
            return None

        if not self.extract_release():
            return None

        release_path = self.get_release_path()
        logger.info(f"UMLS data for version {self.version} is ready at: {release_path}")
        return release_path
