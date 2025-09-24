# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Jules was here
import hashlib
import zipfile
from pathlib import Path
import requests
from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from .config import settings
from rich.console import Console

console = Console()

class UMLSDownloader:
    """
    Handles the download, verification, and extraction of UMLS release files.
    """
    RELEASE_API_URL = "https://uts-ws.nlm.nih.gov/releases"
    DOWNLOAD_API_URL = "https://uts-ws.nlm.nih.gov/download"

    def __init__(self, api_key: str, download_dir: str):
        self.api_key = api_key
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _get_release_info(self, version: str) -> dict:
        """Fetches metadata for a specific UMLS full release version."""
        console.log(f"Fetching UMLS release information for version: [bold cyan]{version}[/bold cyan]...")
        # We don't use 'current=true' so we can find any version
        params = {"releaseType": "umls-full-release"}
        response = requests.get(self.RELEASE_API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if not data["result"]:
            raise ValueError("No UMLS full releases found in API response.")

        for release in data["result"]:
            if release.get("name") == version:
                console.log(f"Found matching release: {release['name']}")
                return release

        raise ValueError(f"UMLS release version '{version}' not found via API. "
                         f"Available versions: {[r.get('name') for r in data['result']]}")


    def _calculate_md5(self, filepath: Path) -> str:
        """Calculates the MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_and_extract_release(self, version: str) -> Path:
        """
        Orchestrates the download, verification, and extraction of a specific UMLS release.
        Returns the path to the extracted META directory.
        Is idempotent: skips download/extraction if already present.
        """
        release_info = self._get_release_info(version)
        download_url = release_info["downloadUrl"]
        expected_checksum = release_info.get("md5")

        # Define paths using the specific version name
        release_version_dir = self.download_dir / version
        zip_filename = Path(download_url).name
        zip_filepath = self.download_dir / zip_filename
        extracted_meta_path = release_version_dir / "META"

        if extracted_meta_path.exists() and extracted_meta_path.is_dir():
            console.log(f"[green]UMLS release {version} already downloaded and extracted at {release_version_dir}. Skipping.[/green]")
            return extracted_meta_path

        # Download
        console.log(f"Downloading {zip_filename}...")
        download_params = {"url": download_url, "apiKey": self.api_key}

        with requests.get(self.DOWNLOAD_API_URL, params=download_params, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                "ETA:", TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task(f"Downloading {zip_filename}", total=total_size)
                with open(zip_filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        progress.update(task, advance=len(chunk))

        console.log(f"Download complete: {zip_filepath}")

        # Verify checksum
        if expected_checksum:
            console.log("Verifying checksum...")
            actual_checksum = self._calculate_md5(zip_filepath)
            if actual_checksum.lower() != expected_checksum.lower():
                raise RuntimeError(
                    f"Checksum mismatch for {zip_filename}. "
                    f"Expected: {expected_checksum}, Got: {actual_checksum}"
                )
            console.log("[green]Checksum verified successfully.[/green]")
        else:
            console.log("[yellow]MD5 checksum not provided in release metadata. Skipping verification.[/yellow]")

        # Extract
        console.log(f"Extracting {zip_filename} to {release_version_dir}...")
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            zip_ref.extractall(release_version_dir)
        console.log("[green]Extraction complete.[/green]")

        # Clean up zip file
        zip_filepath.unlink()
        console.log(f"Removed zip file: {zip_filepath}")

        if not extracted_meta_path.exists():
             raise FileNotFoundError(f"Extracted META directory not found at {extracted_meta_path}")

        return extracted_meta_path

def download_umls_if_needed(version: str) -> Path:
    """
    Entry point function to trigger the UMLS download process using app settings.
    """
    downloader = UMLSDownloader(
        api_key=settings.umls_api_key,
        download_dir=settings.download_dir
    )
    return downloader.download_and_extract_release(version)
