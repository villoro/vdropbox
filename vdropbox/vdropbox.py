import io
from pathlib import Path
from zipfile import ZipFile

import dropbox
import oyaml as yaml
import pandas as pd
from dropbox.files import WriteMode
from loguru import logger


class Vdropbox:
    """Dropbox client for handling files, structured data, and compressed archives."""

    def __init__(self, token, logger=logger):
        """
        Initialize the Dropbox client.

        Args:
            token (str): Dropbox API token.
            logger (logging.Logger, optional): Custom logger instance.
        """
        self.dbx = dropbox.Dropbox(token)
        self.logger = logger

    def _normalize_path(self, path):
        """Ensure paths are in Unix format and start with a `/`."""
        path = str(Path(path).as_posix())  # Convert to Unix format (`/`)
        return f"/{path.lstrip('/')}"  # Ensure it always starts with `/`

    def file_exists(self, filename):
        """Check if a file exists in Dropbox with an exact path match."""
        path = self._normalize_path(filename)
        self.logger.debug(f"Checking if {path=} exists")

        folder, name = (str(Path(path).parent), Path(path).name)
        folder = self._normalize_path(folder)  # Normalize folder path

        self.logger.debug(f"Checking in {folder=}, {name=}")
        search_results = self.dbx.files_search(folder, name).matches

        # Check for exact path match
        for match in search_results:
            if match.metadata.path_lower == path.lower():
                return True

        return False

    def ls(self, folder):
        """List files in a folder."""
        path = self._normalize_path(folder)
        self.logger.debug(f"Checking files in {path=}")
        return sorted(entry.name for entry in self.dbx.files_list_folder(path).entries)

    def delete(self, filename):
        """Delete a file from Dropbox."""
        path = self._normalize_path(filename)
        self.logger.info(f"Deleting '{path}' from Dropbox")
        self.dbx.files_delete(path)

    def move(self, src, dest, overwrite=True):
        """Move (rename) a file in Dropbox."""
        src, dest = self._normalize_path(src), self._normalize_path(dest)
        self.logger.info(f"Moving '{src}' to '{dest}'")

        if overwrite and self.file_exists(dest):
            self.dbx.files_delete(dest)

        self.dbx.files_move(src, dest)

    def _download(self, filename):
        """Download a file from Dropbox."""
        path = self._normalize_path(filename)
        self.logger.info(f"Downloading '{path}' from Dropbox")

        _, res = self.dbx.files_download(path)
        res.raise_for_status()
        return res.content

    def _upload(self, data, filename, as_binary=False):
        """Upload a file to Dropbox."""
        path = self._normalize_path(filename)
        self.logger.info(f"Uploading '{path}' to Dropbox ({len(data)} bytes)")

        if not as_binary:
            data = data.encode()

        with io.BytesIO(data) as stream:
            self.dbx.files_upload(stream.read(), path, mode=WriteMode.overwrite)

    def read_file(self, filename, as_binary=False):
        """Read a text/binary file from Dropbox."""
        content = self._download(filename)
        return content if as_binary else content.decode()

    def write_file(self, content, filename, as_binary=False):
        """Write a text/binary file to Dropbox."""
        self._upload(content, filename, as_binary)

    def read_yaml(self, filename):
        """Read a YAML file from Dropbox."""
        return yaml.safe_load(io.BytesIO(self._download(filename)))

    def write_yaml(self, data, filename, indent=4):
        """Write a YAML file to Dropbox."""
        with io.StringIO() as stream:
            yaml.dump(data, stream, default_flow_style=False, indent=indent)
            self._upload(stream.getvalue(), filename)

    def read_parquet(self, filename, **kwargs):
        """Read a Parquet file into a Pandas DataFrame."""
        return pd.read_parquet(io.BytesIO(self._download(filename)), **kwargs)

    def write_parquet(self, df, filename, **kwargs):
        """Write a Pandas DataFrame to a Parquet file in Dropbox."""
        with io.BytesIO() as stream:
            df.to_parquet(stream, **kwargs)
            self._upload(stream.getvalue(), filename, as_binary=True)

    def read_csv(self, filename, **kwargs):
        """Read a CSV file into a Pandas DataFrame."""
        return pd.read_csv(io.BytesIO(self._download(filename)), **kwargs)

    def write_csv(self, df, filename, **kwargs):
        """Write a Pandas DataFrame to a CSV file in Dropbox."""
        with io.StringIO() as stream:
            df.to_csv(stream, **kwargs)
            self._upload(stream.getvalue(), filename)

    def read_excel(self, filename, sheet_name=None, **kwargs):
        """Read an Excel file into a Pandas DataFrame."""
        with io.BytesIO(self._download(filename)) as stream:
            return pd.read_excel(stream, sheet_name=sheet_name, **kwargs)

    def write_excel(self, df, filename, **kwargs):
        """Write a Pandas DataFrame to an Excel file in Dropbox."""
        with io.BytesIO() as stream:
            with pd.ExcelWriter(stream) as writer:
                df.to_excel(writer, **kwargs)
            self._upload(stream.getvalue(), filename, as_binary=True)

    def read_zip(self, zip_filename, file_inside=None):
        """Read a file inside a ZIP archive in Dropbox."""
        with io.BytesIO(self._download(zip_filename)) as stream:
            with ZipFile(stream) as zip_file:
                file_inside = file_inside or zip_file.namelist()[0]
                return zip_file.read(file_inside)
