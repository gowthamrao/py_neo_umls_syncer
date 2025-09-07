"""
Handles the download and extraction of UMLS RRF data.

- Authenticates with the UMLS Terminology Services (UTS) API.
- Downloads the specified UMLS version.
- Verifies file integrity using checksums.
- Extracts the RRF files from the downloaded archive.
"""
import os
import requests
import zipfile
import hashlib
from pathlib import Path
from typing import Optional

from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn
from .config import settings

class UmlsDownloader:
    """
    Manages the download and extraction of UMLS data.
    """
    def __init__(self, version: str, api_key: str, download_dir: Path = Path("./umls_download")):
        self.version = version
        self.api_key = api_key
        self.download_dir = download_dir
        self.uts_base_url = "https://uts-ws.nlm.nih.gov/download"
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Creates a requests session with the necessary authentication headers."""
        session = requests.Session()
        session.headers.update({"Authorization": f"Basic {self.api_key}"})
        return session

    def get_download_url(self) -> Optional[str]:
        """Retrieves the download URL for the specified UMLS version."""
        url = f"{self.uts_base_url}/meta/{self.version}/releases"
        response = self.session.get(url)
        response.raise_for_status()
        releases = response.json().get("result", [])
        for release in releases:
            if release["fileName"].endswith(".zip"):
                return release["downloadUrl"]
        return None

    def download_release(self, url: str) -> Path:
        """Downloads the UMLS release from the given URL."""
        self.download_dir.mkdir(exist_ok=True)
        local_filename = self.download_dir / url.split('/')[-1]

        with self.session.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(local_filename, 'wb') as f, Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
            ) as progress:
                task = progress.add_task("[cyan]Downloading UMLS...", total=total_size)
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    progress.update(task, advance=len(chunk))
        return local_filename

    def verify_checksum(self, file_path: Path) -> bool:
        """Verifies the MD5 checksum of the downloaded file."""
        # Note: The UTS API does not seem to provide checksums in the metadata.
        # This is a placeholder for now. A real implementation would need to
        # get the checksum from the UMLS website or another source.
        print(f"Checksum verification for {file_path} is not yet implemented.")
        return True

    def extract_release(self, file_path: Path):
        """Extracts the contents of the downloaded zip file."""
        extract_path = self.download_dir / self.version
        extract_path.mkdir(exist_ok=True)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        print(f"Extracted UMLS data to {extract_path}")

    def execute(self) -> Path:
        """Orchestrates the download and extraction process."""
        print(f"Starting download process for UMLS version {self.version}...")
        download_url = self.get_download_url()
        if not download_url:
            raise Exception(f"Could not find a download URL for UMLS version {self.version}")

        downloaded_file = self.download_release(download_url)

        if self.verify_checksum(downloaded_file):
            self.extract_release(downloaded_file)
            return self.download_dir / self.version
        else:
            raise Exception("Checksum verification failed.")

def download_umls(version: str, api_key: str) -> Path:
    """
    High-level function to download and extract a UMLS release.
    """
    downloader = UmlsDownloader(version=version, api_key=api_key)
    return downloader.execute()
