import functools

import backoff
import dropbox
import requests

# Transient errors that are safe to retry
RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,  # includes RemoteDisconnected, SSL errors
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,  # response cut off mid-transfer
    dropbox.exceptions.InternalServerError,  # Dropbox 5xx
)


def _log_retry(details):
    # First positional arg of the decorated method is the Vdropbox instance
    vdp = details["args"][0]
    exc = details.get("exception")
    vdp.logger.warning(
        f"{details['target'].__name__} failed with {type(exc).__name__}, "
        f"retrying in {details['wait']:.1f}s (try {details['tries']})"
    )


def retry_on_network_errors(func):
    """
    Retry a `Vdropbox` method on transient errors using the `backoff` package.

    Connection errors, timeouts and Dropbox 5xx responses are retried with
    exponential backoff (and jitter). Rate limits wait for the `Retry-After`
    hint sent by Dropbox. The number of attempts is `self.max_retries` + 1.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        max_tries = self.max_retries + 1

        decorated = backoff.on_exception(
            backoff.runtime,
            dropbox.exceptions.RateLimitError,
            value=lambda exc: exc.backoff or 1,
            jitter=None,
            max_tries=max_tries,
            on_backoff=_log_retry,
            logger=None,
        )(func)

        decorated = backoff.on_exception(
            backoff.expo,
            RETRYABLE_EXCEPTIONS,
            max_tries=max_tries,
            on_backoff=_log_retry,
            logger=None,
        )(decorated)

        return decorated(self, *args, **kwargs)

    return wrapper
