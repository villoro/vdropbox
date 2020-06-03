import io

import dropbox


class DummyLog:
    """ Dummy log that prints """

    def info(self, msg):
        print(msg)

    def error(self, msg):
        print(msg)


class Vdropbox:
    """Vdropbox object"""

    def __init__(self, token, log=DummyLog()):

        self.dbx = dropbox.Dropbox(token)
        self.log = log

    def write_textfile(text, filename):
        """
            Uploads a text file in dropbox.

            Args:
                text:       text to write
                filename:   name of the file
        """

        with io.BytesIO(text.encode()) as stream:
            stream.seek(0)

            # Write a text file
            self.dbx.files_upload(stream.read(), filename, mode=dropbox.files.WriteMode.overwrite)

        self.log.info(f"File '{filename}' exported to dropbox")

    def read_textfile(filename):
        """
            Reads a text file in dropbox.

            Args:
                filename:   name of the file
        """

        _, res = self.dbx.files_download(filename)

        res.raise_for_status()

        with io.BytesIO(res.content) as stream:
            txt = stream.read().decode()
