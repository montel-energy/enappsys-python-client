"""Keep credentials out of logs written by the HTTP libraries underneath.

The platform authenticates with `user` and `pass` query parameters, so every
request URL carries them. This client already keeps them out of its own
records -- `Session._safe_params` drops the secret and `build_url` returns the
URL without a query string -- but urllib3 logs the request line it sends, which
this client neither formats nor controls:

    urllib3.connectionpool DEBUG https://app.enappsys.com:443
        "GET /csvapi?type=...&user=me&pass=secret HTTP/1.1" 200 None

That only appears when something enables DEBUG logging, but that is a normal
thing to do while debugging a request, and in an orchestrator it means the
secret is written to wherever task logs are kept.

Only the secret is removed. The username is left in place here and in the
client's own log records: it identifies which account made the request, which
is what makes a log line traceable, and it discloses nothing on its own.

A filter is used rather than a formatter because filters are attached to the
logger rather than to a handler, so the redaction applies no matter how the
host application has configured its handlers.
"""

from __future__ import annotations

import logging
import re

#: Secret query parameters, matched wherever they appear in a URL. The
#: username is deliberately not one of them: it identifies who made the
#: request, which is what makes a log line traceable, and it is not a secret.
_CREDENTIAL_PARAMS = ("pass", "secret")

_CREDENTIAL_PATTERN = re.compile(
    r"(?i)\b(" + "|".join(_CREDENTIAL_PARAMS) + r")=([^&\s\"'<>]*)"
)

#: Loggers observed to write request URLs. urllib3 backs `requests`, which the
#: synchronous client uses. aiohttp, used by the asynchronous client, was not
#: observed to log URLs, but is covered in case that changes.
_TARGET_LOGGERS = ("urllib3.connectionpool", "aiohttp.client")


def redact(text: str) -> str:
    """Replace credential query parameter values in `text`."""
    return _CREDENTIAL_PATTERN.sub(r"\1=<redacted>", text)


class CredentialFilter(logging.Filter):
    """Rewrites records whose message carries credentials in a URL."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            # A malformed record is the owning library's problem, not ours, and
            # dropping it here would hide it. Pass it through untouched.
            return True

        if not any(f"{param}=" in message.lower() for param in _CREDENTIAL_PARAMS):
            return True

        # Collapse the record to its formatted text, because the credential may
        # sit inside any one of the args and re-formatting later would undo the
        # substitution.
        record.msg = redact(message)
        record.args = ()
        return True


def install() -> None:
    """Attach the filter to the HTTP libraries' loggers, once.

    Called when a session is created rather than at import, so that importing
    the package does not reconfigure logging for an application that never
    makes a request.
    """
    for name in _TARGET_LOGGERS:
        logger = logging.getLogger(name)
        if not any(isinstance(f, CredentialFilter) for f in logger.filters):
            logger.addFilter(CredentialFilter())
