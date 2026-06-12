from unittest.mock import MagicMock
from unittest.mock import patch

import dropbox
import pytest
import requests

from vdropbox import Vdropbox


@pytest.fixture
def vdp():
    with patch("vdropbox.vdropbox.dropbox.Dropbox") as mock_cls:
        client = Vdropbox("fake_token")
        client.dbx = mock_cls.return_value
        yield client


@pytest.fixture
def sleep():
    with patch("backoff._sync.time.sleep") as mock_sleep:
        yield mock_sleep


def connection_error():
    return requests.exceptions.ConnectionError("Connection aborted.")


def test_retries_then_succeeds(vdp, sleep):
    vdp.dbx.files_delete_v2.side_effect = [
        connection_error(),
        connection_error(),
        MagicMock(),
    ]
    vdp.delete("file.txt")  # should not raise
    assert vdp.dbx.files_delete_v2.call_count == 3
    # Exponential backoff with full jitter: waits within [0, 1] then [0, 2]
    waits = [call.args[0] for call in sleep.call_args_list]
    assert len(waits) == 2
    assert 0 <= waits[0] <= 1
    assert 0 <= waits[1] <= 2


def test_gives_up_after_max_retries(vdp, sleep):
    vdp.max_retries = 2
    vdp.dbx.files_delete_v2.side_effect = connection_error()
    with pytest.raises(requests.exceptions.ConnectionError):
        vdp.delete("file.txt")
    assert vdp.dbx.files_delete_v2.call_count == 3  # 1 try + 2 retries


def test_no_retries_when_disabled(vdp, sleep):
    vdp.max_retries = 0
    vdp.dbx.files_delete_v2.side_effect = connection_error()
    with pytest.raises(requests.exceptions.ConnectionError):
        vdp.delete("file.txt")
    assert vdp.dbx.files_delete_v2.call_count == 1
    sleep.assert_not_called()


def test_timeout_is_retried(vdp, sleep):
    vdp.dbx.files_delete_v2.side_effect = [
        requests.exceptions.Timeout("timed out"),
        MagicMock(),
    ]
    vdp.delete("file.txt")
    assert vdp.dbx.files_delete_v2.call_count == 2


def test_rate_limit_honors_backoff_hint(vdp, sleep):
    error = MagicMock()
    error.is_path.return_value = False
    rate_limit = dropbox.exceptions.RateLimitError("rid", error, backoff=7)
    vdp.dbx.files_delete_v2.side_effect = [rate_limit, MagicMock()]

    vdp.delete("file.txt")
    sleep.assert_called_once_with(7)


def test_non_retryable_error_raises_immediately(vdp, sleep):
    vdp.dbx.files_delete_v2.side_effect = ValueError("boom")
    with pytest.raises(ValueError):
        vdp.delete("file.txt")
    assert vdp.dbx.files_delete_v2.call_count == 1
    sleep.assert_not_called()
