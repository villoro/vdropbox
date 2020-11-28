import io

import dropbox
import oyaml as yaml
import pandas as pd

from dropbox.files import WriteMode


class DummyLog:
    """ Dummy log that prints """

    def info(self, msg):
        print(msg)

    def warning(self, msg):
        print(msg)

    def error(self, msg):
        print(msg)


class Vdropbox:
    """Vdropbox object"""

    def __init__(self, token, log=DummyLog()):
        """
            Creates the vdropbox object

            Args:
                token:  for connection to dropbox
                log:    log object. The default one only prints
        """

        self.dbx = dropbox.Dropbox(token)
        self.log = log

    def _check_one_file(self, path, filename):
        """
            Internal function to check if a file exists in dropbox

            Args:
                path:       folder of the file
                filename:   name of the file
        """

        for match in self.dbx.files_search(path, filename).matches:
            if filename == match.metadata.name:
                return True

        return False

    def _check_path(self, path):
        """ Check that a path starts with '/'"""

        if not path.startswith("/"):
            path = "/" + path

        return path

    def file_exists(self, uri):
        """
            Check if a file exists in dropbox

            Args:
                uri:    file uri
        """

        # Check all folders before the actual file
        data = uri.split("/")
        for i, filename in enumerate(data):
            path = "/".join(data[:i])

            # Skip if filename is empty (happens when uri starts with '/')
            if not filename:
                continue

            # All non empty path must start with '/'
            if path and not path.startswith("/"):
                path = "/" + path

            # If one file or folder don't exist return False
            if not self._check_one_file(path, filename):
                return False

        # If all exists return true
        return True

    def ls(self, folder):
        """ List entries in a folder """

        folder = self._check_path(folder)

        return sorted([x.name for x in self.dbx.files_list_folder(folder).entries])

    def delete(self, filename):
        """ Delete a file/folder from dropbox """

        filename = self._check_path(filename)

        self.dbx.files_delete(filename)
        self.log.info(f"File '{filename}' deleted dropbox")

    def _raw_read(self, filename):
        """ Auxiliar function for reading from dropbox """

        filename = self._check_path(filename)

        _, res = self.dbx.files_download(filename)
        res.raise_for_status()
        return res.content

    def read_file(self, filename):
        """
            Reads a text file in dropbox.

            Args:
                filename:   name of the file
        """

        content = self._raw_read(filename)

        with io.BytesIO(content) as stream:
            return stream.read().decode()

    def write_file(self, text, filename):
        """
            Uploads a text file in dropbox.

            Args:
                text:       text to write
                filename:   name of the file
        """

        if not filename.startswith("/"):
            filename = "/" + filename

        with io.BytesIO(text.encode()) as stream:
            stream.seek(0)

            # Write a text file
            self.dbx.files_upload(stream.read(), filename, mode=WriteMode.overwrite)

        self.log.info(f"File '{filename}' exported to dropbox")

    def read_yaml(self, filename):
        """
            Read a yaml from dropbox as an ordered dict

            Args:
                filename:   name of the yaml file
        """

        content = self._raw_read(filename)

        with io.BytesIO(content) as stream:
            return yaml.safe_load(stream)

    def write_yaml(self, data, filename):
        """
            Uploads a dict/ordered dict as yaml in dropbox.

            Args:
                data:       dict or dict-like info
                filename:   name of the yaml file
        """

        filename = self._check_path(filename)

        with io.StringIO() as stream:
            yaml.dump(data, stream, default_flow_style=False, indent=4)
            stream.seek(0)

            self.dbx.files_upload(stream.read().encode(), filename, mode=WriteMode.overwrite)

        self.log.info(f"File '{filename}' exported to dropbox")

    def read_parquet(self, filename, **kwa):
        """
            Read a parquet from dropbox as a pandas dataframe

            Args:
                filename:   name of the parquet file
                **kwa:      keyworded arguments for the pd.read_parquet inner function
        """

        content = self._raw_read(filename)

        with io.BytesIO(content) as stream:
            return pd.read_parquet(stream, **kwa)

    def write_parquet(self, df, filename, **kwa):
        """
            Write a parquet to dropbox from a pandas dataframe.

            Args:
                df:         pandas dataframe
                filename:   name of the yaml file
                **kwa:      keyworded arguments for the pd.to_parquet inner function
        """

        filename = self._check_path(filename)

        with io.BytesIO() as stream:
            df.to_parquet(stream, **kwa)
            stream.seek(0)

            self.dbx.files_upload(stream.getvalue(), filename, mode=WriteMode.overwrite)

        self.log.info(f"File '{filename}' exported to dropbox")

    def read_excel(self, filename, sheet_names=None, **kwa):
        """
            Read an excel from dropbox as a pandas dataframe

            Args:
                filename:       name of the excel file
                sheet_names:    names of the sheets to read (if None read the only sheet)
                **kwa:          keyworded arguments for the pd.read_excel inner function
        """

        content = self._raw_read(filename)

        # Read one dataframe
        if sheet_names is None:
            with io.BytesIO(content) as stream:
                return pd.read_excel(stream, **kwa)

        # Read multiple dataframes
        with io.BytesIO(content) as stream:
            return {x: pd.read_excel(stream, sheet_name=x, **kwa) for x in sheet_names}

    def write_excel(self, df, filename, **kwa):
        """
            Write an excel to dropbox from a pandas dataframe

            Args:
                filename:   name of the excel file
                **kwa:      keyworded arguments for the df.to_excel inner function
        """

        filename = self._check_path(filename)

        with io.BytesIO() as stream:
            writer = pd.ExcelWriter(stream)
            df.to_excel(writer, **kwa)

            writer.save()
            stream.seek(0)

            self.dbx.files_upload(stream.getvalue(), filename, mode=WriteMode.overwrite)

        self.log.info(f"File '{filename}' exported to dropbox")
