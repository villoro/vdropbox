import io

import dropbox


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

        if not folder.startswith("/"):
            folder = "/" + folder

        return sorted([x.name for x in self.dbx.files_list_folder(folder).entries])

    def delete(self, uri):
        """ Delete a file/folder from dropbox """

        if not uri.startswith("/"):
            uri = "/" + uri

        self.dbx.files_delete(uri)
        self.log.info(f"File '{uri}' deleted dropbox")

    def _raw_read(self, filename):
        """ Auxiliar function for reading from dropbox """

        if not filename.startswith("/"):
            filename = "/" + filename

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
            self.dbx.files_upload(stream.read(), filename, mode=dropbox.files.WriteMode.overwrite)

        self.log.info(f"File '{filename}' exported to dropbox")
