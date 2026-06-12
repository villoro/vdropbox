import io
import json
from zipfile import ZipFile

import dropbox
import yaml
from dropbox.files import FolderMetadata
from dropbox.files import WriteMode
from loguru import logger as _default_logger

from vdropbox.retry import retry_on_network_errors


def _require_pandas():
    """Import pandas lazily so it stays an optional dependency."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required for this method. Install it with `pip install vdropbox[pandas]`"
        ) from exc
    return pd


class Vdropbox:
    """Dropbox client for handling files, structured data, and compressed archives."""

    def __init__(self, token: str, logger=_default_logger, max_retries: int = 4):
        """
        Initialize the Dropbox client.

        Args:
            token: Dropbox API token.
            logger: Custom logger instance (defaults to loguru).
            max_retries: Retries on transient network/server errors (with
                exponential backoff). Set to 0 to disable.
        """
        self.dbx = dropbox.Dropbox(token)
        self.logger = logger
        self.max_retries = max_retries

    def _normalize_path(self, path: str) -> str:
        """Ensure paths are in Unix format and start with a `/`."""
        # Avoid pathlib here: on Windows it treats `//x` as a UNC root
        parts = str(path).replace("\\", "/").split("/")
        return "/" + "/".join(p for p in parts if p)

    @retry_on_network_errors
    def file_exists(self, filename: str) -> bool:
        """Check if a file or folder exists in Dropbox with an exact path match."""
        path = self._normalize_path(filename)
        self.logger.debug(f"Checking if {path=} exists")

        try:
            self.dbx.files_get_metadata(path)
            return True
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                return False
            raise

    def _list_entries(self, path: str, recursive: bool = False):
        """Yield all entries in a folder, handling pagination."""
        result = self.dbx.files_list_folder(path, recursive=recursive)
        yield from result.entries
        while result.has_more:
            result = self.dbx.files_list_folder_continue(result.cursor)
            yield from result.entries

    @retry_on_network_errors
    def ls(self, folder: str, recursive: bool = False) -> list[str]:
        """
        List files and folders in a folder.

        Args:
            folder: Folder to list.
            recursive: If True, list all nested entries and return full paths
                (relative to `folder`, without a leading `/`). If False, return names only.
        """
        path = self._normalize_path(folder)
        self.logger.debug(f"Checking files in {path=} ({recursive=})")

        entries = self._list_entries(path, recursive=recursive)
        if not recursive:
            return sorted(entry.name for entry in entries)

        prefix = "" if path == "/" else path.lower()
        out = []
        for entry in entries:
            # In recursive mode skip intermediate folders, keep only files
            if isinstance(entry, FolderMetadata):
                continue
            full_path = entry.path_display
            out.append(full_path[len(prefix) :].lstrip("/"))
        return sorted(out)

    @retry_on_network_errors
    def delete(self, filename: str):
        """Delete a file from Dropbox."""
        path = self._normalize_path(filename)
        self.logger.info(f"Deleting '{path}' from Dropbox")
        self.dbx.files_delete_v2(path)

    @retry_on_network_errors
    def move(self, src: str, dest: str, overwrite: bool = True):
        """Move (rename) a file in Dropbox."""
        src, dest = self._normalize_path(src), self._normalize_path(dest)
        self.logger.info(f"Moving '{src}' to '{dest}'")

        if overwrite and self.file_exists(dest):
            self.dbx.files_delete_v2(dest)

        self.dbx.files_move_v2(src, dest)

    @retry_on_network_errors
    def _download(self, filename: str) -> bytes:
        """Download a file from Dropbox."""
        path = self._normalize_path(filename)
        self.logger.info(f"Downloading '{path}' from Dropbox")

        _, res = self.dbx.files_download(path)
        res.raise_for_status()
        return res.content

    @retry_on_network_errors
    def _upload(self, data, filename: str, as_binary: bool = False):
        """Upload a file to Dropbox."""
        path = self._normalize_path(filename)

        if not as_binary:
            data = data.encode()

        self.logger.info(f"Uploading '{path}' to Dropbox ({len(data)} bytes)")
        self.dbx.files_upload(data, path, mode=WriteMode.overwrite)

    def read_file(self, filename: str, as_binary: bool = False):
        """Read a text/binary file from Dropbox."""
        content = self._download(filename)
        return content if as_binary else content.decode()

    def write_file(self, content, filename: str, as_binary: bool = False):
        """Write a text/binary file to Dropbox."""
        self._upload(content, filename, as_binary)

    def read_yaml(self, filename: str):
        """Read a YAML file from Dropbox."""
        return yaml.safe_load(io.BytesIO(self._download(filename)))

    def write_yaml(self, data, filename: str, indent: int = 4):
        """Write a YAML file to Dropbox."""
        content = yaml.safe_dump(
            data, default_flow_style=False, indent=indent, sort_keys=False
        )
        self._upload(content, filename)

    def read_json(self, filename: str, **kwargs):
        """Read a JSON file from Dropbox."""
        return json.loads(self._download(filename), **kwargs)

    def write_json(self, data, filename: str, indent: int = 4, **kwargs):
        """Write a JSON file to Dropbox."""
        self._upload(json.dumps(data, indent=indent, **kwargs), filename)

    def read_parquet(self, filename: str, **kwargs):
        """Read a Parquet file into a Pandas DataFrame."""
        pd = _require_pandas()
        return pd.read_parquet(io.BytesIO(self._download(filename)), **kwargs)

    def write_parquet(self, df, filename: str, **kwargs):
        """Write a Pandas DataFrame to a Parquet file in Dropbox."""
        with io.BytesIO() as stream:
            df.to_parquet(stream, **kwargs)
            self._upload(stream.getvalue(), filename, as_binary=True)

    def read_csv(self, filename: str, **kwargs):
        """Read a CSV file into a Pandas DataFrame."""
        pd = _require_pandas()
        return pd.read_csv(io.BytesIO(self._download(filename)), **kwargs)

    def write_csv(self, df, filename: str, **kwargs):
        """Write a Pandas DataFrame to a CSV file in Dropbox."""
        with io.StringIO() as stream:
            df.to_csv(stream, **kwargs)
            self._upload(stream.getvalue(), filename)

    def read_excel(self, filename: str, sheet_name=0, **kwargs):
        """Read an Excel file into a Pandas DataFrame."""
        pd = _require_pandas()
        with io.BytesIO(self._download(filename)) as stream:
            return pd.read_excel(stream, sheet_name=sheet_name, **kwargs)

    def write_excel(self, df, filename: str, **kwargs):
        """Write a Pandas DataFrame to an Excel file in Dropbox."""
        pd = _require_pandas()
        with io.BytesIO() as stream:
            with pd.ExcelWriter(stream) as writer:
                df.to_excel(writer, **kwargs)
            self._upload(stream.getvalue(), filename, as_binary=True)

    def read_zip(self, zip_filename: str, file_inside: str | None = None) -> bytes:
        """Read a file inside a ZIP archive in Dropbox."""
        with io.BytesIO(self._download(zip_filename)) as stream:
            with ZipFile(stream) as zip_file:
                file_inside = file_inside or zip_file.namelist()[0]
                return zip_file.read(file_inside)

    @retry_on_network_errors
    def mkdir_p(self, folder: str):
        """Create a folder (and any missing parents), like `mkdir -p`."""
        path = self._normalize_path(folder)
        self.logger.info(f"Creating {path=} (mkdir_p)")

        try:
            self.dbx.files_create_folder_v2(path)
        except dropbox.exceptions.ApiError as e:
            # Dropbox creates missing parents automatically; conflict means it already exists
            if e.error.is_path() and e.error.get_path().is_conflict():
                return
            raise
