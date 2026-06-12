import io
import json
import zipfile
from unittest.mock import MagicMock
from unittest.mock import patch

import dropbox
import pytest

from vdropbox import Vdropbox


def make_api_error(is_path=True, path_check=None):
    """Build a dropbox ApiError whose error matches the given path check."""
    error = MagicMock()
    error.is_path.return_value = is_path
    path_error = MagicMock()
    path_error.is_not_found.return_value = path_check == "not_found"
    path_error.is_conflict.return_value = path_check == "conflict"
    error.get_path.return_value = path_error
    return dropbox.exceptions.ApiError("request_id", error, "msg", "locale")


@pytest.fixture
def vdp():
    with patch("vdropbox.vdropbox.dropbox.Dropbox") as mock_cls:
        client = Vdropbox("fake_token")
        client.dbx = mock_cls.return_value
        yield client


def mock_download(vdp, content: bytes):
    res = MagicMock()
    res.content = content
    vdp.dbx.files_download.return_value = (MagicMock(), res)


def uploaded_bytes(vdp) -> bytes:
    args, _ = vdp.dbx.files_upload.call_args
    return args[0]


class TestNormalizePath:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("file.txt", "/file.txt"),
            ("/file.txt", "/file.txt"),
            ("folder/file.txt", "/folder/file.txt"),
            ("folder\\file.txt", "/folder/file.txt"),
            ("//folder/file.txt", "/folder/file.txt"),
        ],
    )
    def test_normalize(self, vdp, raw, expected):
        assert vdp._normalize_path(raw) == expected


class TestFileExists:
    def test_exists(self, vdp):
        vdp.dbx.files_get_metadata.return_value = MagicMock()
        assert vdp.file_exists("file.txt") is True
        vdp.dbx.files_get_metadata.assert_called_once_with("/file.txt")

    def test_not_found(self, vdp):
        vdp.dbx.files_get_metadata.side_effect = make_api_error(path_check="not_found")
        assert vdp.file_exists("missing.txt") is False

    def test_other_error_raises(self, vdp):
        vdp.dbx.files_get_metadata.side_effect = make_api_error(is_path=False)
        with pytest.raises(dropbox.exceptions.ApiError):
            vdp.file_exists("file.txt")


def make_entry(name, path_display, is_folder=False):
    cls = dropbox.files.FolderMetadata if is_folder else dropbox.files.FileMetadata
    entry = MagicMock(spec=cls)
    entry.name = name
    entry.path_display = path_display
    return entry


class TestLs:
    def test_simple(self, vdp):
        result = MagicMock()
        result.entries = [
            make_entry("b.txt", "/f/b.txt"),
            make_entry("a.txt", "/f/a.txt"),
        ]
        result.has_more = False
        vdp.dbx.files_list_folder.return_value = result

        assert vdp.ls("f") == ["a.txt", "b.txt"]

    def test_pagination(self, vdp):
        page1 = MagicMock()
        page1.entries = [make_entry("a.txt", "/f/a.txt")]
        page1.has_more = True
        page1.cursor = "cur"
        page2 = MagicMock()
        page2.entries = [make_entry("b.txt", "/f/b.txt")]
        page2.has_more = False
        vdp.dbx.files_list_folder.return_value = page1
        vdp.dbx.files_list_folder_continue.return_value = page2

        assert vdp.ls("f") == ["a.txt", "b.txt"]
        vdp.dbx.files_list_folder_continue.assert_called_once_with("cur")

    def test_recursive(self, vdp):
        result = MagicMock()
        result.entries = [
            make_entry("sub", "/f/sub", is_folder=True),
            make_entry("x.txt", "/f/sub/x.txt"),
            make_entry("a.txt", "/f/a.txt"),
        ]
        result.has_more = False
        vdp.dbx.files_list_folder.return_value = result

        assert vdp.ls("f", recursive=True) == ["a.txt", "sub/x.txt"]
        vdp.dbx.files_list_folder.assert_called_once_with("/f", recursive=True)


class TestDeleteMove:
    def test_delete(self, vdp):
        vdp.delete("file.txt")
        vdp.dbx.files_delete_v2.assert_called_once_with("/file.txt")

    def test_move_overwrite(self, vdp):
        vdp.dbx.files_get_metadata.return_value = MagicMock()  # dest exists
        vdp.move("a.txt", "b.txt")
        vdp.dbx.files_delete_v2.assert_called_once_with("/b.txt")
        vdp.dbx.files_move_v2.assert_called_once_with("/a.txt", "/b.txt")

    def test_move_no_overwrite(self, vdp):
        vdp.move("a.txt", "b.txt", overwrite=False)
        vdp.dbx.files_delete_v2.assert_not_called()
        vdp.dbx.files_move_v2.assert_called_once_with("/a.txt", "/b.txt")


class TestReadWriteFile:
    def test_read_text(self, vdp):
        mock_download(vdp, b"hello")
        assert vdp.read_file("file.txt") == "hello"

    def test_read_binary(self, vdp):
        mock_download(vdp, b"\x00\x01")
        assert vdp.read_file("file.bin", as_binary=True) == b"\x00\x01"

    def test_write_text(self, vdp):
        vdp.write_file("hello", "file.txt")
        assert uploaded_bytes(vdp) == b"hello"

    def test_write_binary(self, vdp):
        vdp.write_file(b"\x00\x01", "file.bin", as_binary=True)
        assert uploaded_bytes(vdp) == b"\x00\x01"


class TestYamlJson:
    def test_yaml_roundtrip(self, vdp):
        data = {"b": 1, "a": [1, 2]}
        vdp.write_yaml(data, "f.yaml")
        mock_download(vdp, uploaded_bytes(vdp))
        assert vdp.read_yaml("f.yaml") == data

    def test_yaml_preserves_key_order(self, vdp):
        vdp.write_yaml({"z": 1, "a": 2}, "f.yaml")
        content = uploaded_bytes(vdp).decode()
        assert content.index("z:") < content.index("a:")

    def test_json_roundtrip(self, vdp):
        data = {"a": 1, "b": [1, 2], "c": "x"}
        vdp.write_json(data, "f.json")
        mock_download(vdp, uploaded_bytes(vdp))
        assert vdp.read_json("f.json") == data

    def test_json_is_indented(self, vdp):
        vdp.write_json({"a": 1}, "f.json")
        assert uploaded_bytes(vdp).decode() == json.dumps({"a": 1}, indent=4)


class TestPandas:
    def test_csv_roundtrip(self, vdp):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        vdp.write_csv(df, "f.csv", index=False)
        mock_download(vdp, uploaded_bytes(vdp))
        pd.testing.assert_frame_equal(vdp.read_csv("f.csv"), df)

    def test_parquet_roundtrip(self, vdp):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        vdp.write_parquet(df, "f.parquet")
        mock_download(vdp, uploaded_bytes(vdp))
        pd.testing.assert_frame_equal(vdp.read_parquet("f.parquet"), df)

    def test_excel_roundtrip(self, vdp):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("openpyxl")
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        vdp.write_excel(df, "f.xlsx", index=False)
        mock_download(vdp, uploaded_bytes(vdp))
        pd.testing.assert_frame_equal(vdp.read_excel("f.xlsx"), df)


class TestZip:
    def test_read_zip(self, vdp):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("inner.txt", "content")
        mock_download(vdp, buf.getvalue())

        assert vdp.read_zip("archive.zip") == b"content"
        assert vdp.read_zip("archive.zip", "inner.txt") == b"content"


class TestMkdirP:
    def test_creates_folder(self, vdp):
        vdp.mkdir_p("a/b/c")
        vdp.dbx.files_create_folder_v2.assert_called_once_with("/a/b/c")

    def test_existing_folder_is_ok(self, vdp):
        vdp.dbx.files_create_folder_v2.side_effect = make_api_error(
            path_check="conflict"
        )
        vdp.mkdir_p("a/b")  # should not raise

    def test_other_error_raises(self, vdp):
        vdp.dbx.files_create_folder_v2.side_effect = make_api_error(is_path=False)
        with pytest.raises(dropbox.exceptions.ApiError):
            vdp.mkdir_p("a/b")
