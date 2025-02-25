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
        """Initialize the Dropbox client."""
        self.dbx = dropbox.Dropbox(token)
        self.logger = logger

    def file_exists(self, uri):
        """Check if a file exists in Dropbox."""
        path = Path(uri)
        folder, filename = str(path.parent), path.name

        for match in self.dbx.files_search(folder, filename).matches:
            if filename == match.metadata.name:
                return True
        return False

    def ls(self, folder):
        """List files in a folder."""
        folder = str(Path(folder).resolve())
        return sorted(
            entry.name for entry in self.dbx.files_list_folder(folder).entries
        )

    def delete(self, filename):
        """Delete a file from Dropbox."""
        self.logger.info(f"Deleting '{filename}' from Dropbox")
        self.dbx.files_delete(str(Path(filename).resolve()))

    def move(self, src, dest, overwrite=True):
        """Move (rename) a file in Dropbox."""
        self.logger.info(f"Moving '{src}' to '{dest}'")

        if overwrite and self.file_exists(dest):
            self.dbx.files_delete(dest)

        self.dbx.files_copy(src, dest)
        self.dbx.files_delete(src)

    def _download(self, filename):
        """Download a file from Dropbox."""
        self.logger.info(f"Reading {filename=} from Dropbox")

        _, res = self.dbx.files_download(str(Path(filename).resolve()))
        res.raise_for_status()
        return res.content

    def _upload(self, data, filename, as_binary=False):
        """Upload a file to Dropbox."""
        self.logger.info(f"Uploading {filename=} to Dropbox")

        if not as_binary:
            data = data.encode()

        path = str(Path(filename).resolve())
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
        content = self._download(filename)
        return yaml.safe_load(io.BytesIO(content))

    def write_yaml(self, data, filename):
        """Write a YAML file to Dropbox."""
        with io.StringIO() as stream:
            yaml.dump(data, stream, default_flow_style=False, indent=4)
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
        content = self._download(filename)
        with io.BytesIO(content) as stream:
            return pd.read_excel(stream, sheet_name=sheet_name, **kwargs)

    def write_excel(self, df, filename, **kwargs):
        """Write a Pandas DataFrame to an Excel file in Dropbox."""
        with io.BytesIO() as stream:
            with pd.ExcelWriter(stream) as writer:
                df.to_excel(writer, **kwargs)
            self._upload(stream.getvalue(), filename, as_binary=True)

    def read_zip(self, zip_filename, file_inside=None):
        """Read a file inside a ZIP archive in Dropbox."""
        content = self._download(zip_filename)
        with io.BytesIO(content) as stream:
            with ZipFile(stream) as zip_file:
                file_inside = file_inside or zip_file.namelist()[0]
                return zip_file.read(file_inside)
