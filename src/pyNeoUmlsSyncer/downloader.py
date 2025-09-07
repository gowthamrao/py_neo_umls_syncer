"""
downloader.py

This module is responsible for acquiring the UMLS Rich Release Format (RRF) data.
It handles:
- Authenticating with the UMLS UTS API to get service tickets.
- Downloading the specified UMLS release zip file.
- Verifying the checksum of the downloaded file (placeholder for now).
- Extracting the necessary RRF files for parsing.
"""
import logging
import zipfile
from pathlib import Path
from typing import Dict, Any

import requests
from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn

from .config import Settings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UmlsAuthenticationError(Exception):
    """Custom exception for UMLS authentication failures."""
    pass

class UmlsDownloader:
    """
    A class to handle the download and extraction of UMLS release files.
    """
    _AUTH_URL = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
    _DOWNLOAD_SERVICE_URL = "http://download.nlm.nih.gov"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.data_dir = Path(self.settings.data_dir)
        self.version = self.settings.umls_version
        self.api_key = self.settings.umls_api_key

    def _get_ticket_granting_ticket(self) -> str:
        """
        Authenticates with the UMLS API to get a Ticket-Granting Ticket (TGT).
        """
        logger.info("Requesting UMLS Ticket-Granting Ticket (TGT)...")
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        params = {'apikey': self.api_key}
        response = requests.post(self._AUTH_URL, headers=headers, data=params)
        response.raise_for_status()
        # The response text contains the TGT URI
        tgt_uri = response.headers['Location']
        logger.info("Successfully obtained TGT.")
        return tgt_uri

    def _get_service_ticket(self, tgt_uri: str) -> str:
        """
        Uses the TGT to get a single-use Service Ticket (ST) for the download service.
        """
        logger.info("Requesting UMLS Service Ticket (ST)...")
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        params = {'service': self._DOWNLOAD_SERVICE_URL}
        response = requests.post(tgt_uri, headers=headers, data=params)
        response.raise_for_status()
        logger.info("Successfully obtained ST.")
        return response.text

    def _get_download_url(self) -> str:
        """
        Constructs the download URL for the specified UMLS version.
        This is a common pattern but might need adjustment for future releases.
        """
        # Note: This URL structure is based on current patterns.
        # It might require updates if NLM changes its URL schema.
        # Example: https://download.nlm.nih.gov/umls/kss/2024AA/umls-2024AA-full.zip
        return (
            f"{self._DOWNLOAD_SERVICE_URL}/umls/kss/{self.version}/"
            f"umls-{self.version}-full.zip"
        )

    def download_and_extract_release(self):
        """
        Orchestrates the download and extraction of the UMLS release.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        zip_path = self.data_dir / f"umls-{self.version}-full.zip"
        release_dir = self.data_dir / self.version

        if release_dir.exists():
            logger.info(f"UMLS release {self.version} already extracted in {release_dir}. Skipping download.")
            return

        try:
            tgt = self._get_ticket_granting_ticket()
            service_ticket = self._get_service_ticket(tgt)
        except requests.exceptions.RequestException as e:
            raise UmlsAuthenticationError(f"Failed to authenticate with UMLS API: {e}")

        download_url = self._get_download_url()
        params = {'ticket': service_ticket}

        logger.info(f"Starting download of UMLS version {self.version} from {download_url}")

        try:
            with requests.get(download_url, params=params, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))

                with Progress(
                    "[progress.description]{task.description}",
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                ) as progress:
                    task = progress.add_task(f"Downloading {zip_path.name}", total=total_size)
                    with open(zip_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))
            logger.info("Download complete.")

            # TODO: Implement checksum verification.
            # This would involve fetching the checksum file and comparing it.
            # For now, we proceed directly to extraction.

            logger.info(f"Extracting {zip_path.name} to {release_dir}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # We only need the RRF files from the META directory.
                for member in zip_ref.infolist():
                    if "META/" in member.filename and member.filename.endswith('.RRF'):
                        zip_ref.extract(member, self.data_dir)

            # The zip file extracts into a folder named after the version, e.g., "2024AA".
            # We want to move the RRF files from the "META" subdirectory to the root of our versioned folder.
            extracted_root_dir = self.data_dir / self.version
            source_meta_dir = extracted_root_dir / "META"

            if source_meta_dir.is_dir():
                logger.info(f"Moving RRF files from {source_meta_dir} to {release_dir}...")
                for rrf_file in source_meta_dir.glob("*.RRF"):
                    rrf_file.rename(release_dir / rrf_file.name)
                # Clean up the now-empty META directory
                source_meta_dir.rmdir()

                # The zip may contain other directories (e.g., 'LEX') which we can also remove if empty.
                # For now, we just clean up META.
            else:
                logger.warning(f"Could not find 'META' subdirectory in {extracted_root_dir}. Files might be in the root.")

            logger.info("Extraction complete.")

        finally:
            # Clean up the downloaded zip file to save space
            if zip_path.exists():
                logger.info(f"Cleaning up downloaded file: {zip_path}")
                zip_path.unlink()

if __name__ == '__main__':
    # Example usage:
    # Requires a .env file with UMLS_API_KEY and UMLS_VERSION
    try:
        settings = Settings()
        downloader = UmlsDownloader(settings)
        downloader.download_and_extract_release()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
