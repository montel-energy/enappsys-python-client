"""Secrets must not reach logs written by the HTTP libraries underneath.

The platform authenticates with query parameters, so every request URL carries
them. This client keeps the secret out of its own records, but urllib3 logs the
request line it sends -- a line this client neither formats nor controls.

The username is deliberately kept everywhere: it identifies the account behind
a request, which is what makes a log traceable, and it is not a secret.
"""

from __future__ import annotations

import logging

import pytest

from enappsys.redaction import CredentialFilter, install, redact

# The line urllib3 actually emitted, with the credentials replaced.
LEAKED_LINE = (
    'https://app.enappsys.com:443 "GET /csvapi?type=ENTSOE_AGGREGATED_GENERATION'
    "_PER_TYPE&entities=DE.GERMANY_SOLAR&start=202606201200&end=202609181200&res"
    '=qh&timezone=UTC&user=someone&pass=sensitive-value HTTP/1.1" 200 None'
)


class TestRedact:
    def test_the_line_urllib3_actually_logged(self):
        cleaned = redact(LEAKED_LINE)
        assert "sensitive-value" not in cleaned
        assert "pass=<redacted>" in cleaned

    def test_the_username_is_kept(self):
        """It identifies the account behind a request and is not a secret;
        stripping it makes a log much harder to trace back."""
        cleaned = redact(LEAKED_LINE)
        assert "user=someone" in cleaned

    def test_everything_else_survives(self):
        """Redaction must not make the log line useless for debugging."""
        cleaned = redact(LEAKED_LINE)
        for kept in (
            "ENTSOE_AGGREGATED_GENERATION_PER_TYPE",
            "DE.GERMANY_SOLAR",
            "start=202606201200",
            "res=qh",
            "user=someone",
            "200 None",
        ):
            assert kept in cleaned

    def test_it_stops_at_the_parameter_boundary(self):
        cleaned = redact("a=1&pass=secret&res=qh")
        assert cleaned == "a=1&pass=<redacted>&res=qh"

    @pytest.mark.parametrize("param", ["pass", "secret", "PASS", "Secret"])
    def test_case_and_parameter_variants(self, param):
        assert "hunter2" not in redact(f"https://x/y?{param}=hunter2&b=2")

    def test_a_parameter_merely_ending_in_pass_is_untouched(self):
        assert redact("?bypass=true") == "?bypass=true"

    def test_text_without_credentials_is_untouched(self):
        line = 'https://app.enappsys.com:443 "GET /csvapi?res=qh HTTP/1.1" 200 None'
        assert redact(line) == line


class TestFilter:
    def test_a_lazily_formatted_record_is_redacted(self):
        """urllib3 logs with %s args rather than a formatted string, so the
        filter has to collapse the record rather than edit `msg` alone."""
        record = logging.LogRecord(
            name="urllib3.connectionpool",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg='%s://%s:%s "%s %s" %s',
            args=("https", "app.enappsys.com", 443, "GET", "/csvapi?pass=secret", 200),
            exc_info=None,
        )
        assert CredentialFilter().filter(record) is True
        assert "secret" not in record.getMessage()
        assert "pass=<redacted>" in record.getMessage()

    def test_records_are_never_dropped(self):
        """The filter redacts; it must not swallow log records."""
        record = logging.LogRecord(
            "urllib3.connectionpool", logging.DEBUG, __file__, 1, "no secrets", None, None
        )
        assert CredentialFilter().filter(record) is True

    def test_a_broken_record_passes_through(self):
        """A record whose args do not match its format is the owning library's
        bug; hiding it here would make that harder to find."""
        record = logging.LogRecord(
            "urllib3.connectionpool", logging.DEBUG, __file__, 1, "%s %s", ("only-one",), None
        )
        assert CredentialFilter().filter(record) is True


class TestInstall:
    def test_it_attaches_to_the_http_loggers(self):
        install()
        for name in ("urllib3.connectionpool", "aiohttp.client"):
            filters = logging.getLogger(name).filters
            assert any(isinstance(f, CredentialFilter) for f in filters), name

    def test_installing_twice_does_not_stack_filters(self):
        install()
        install()
        install()
        filters = logging.getLogger("urllib3.connectionpool").filters
        assert sum(isinstance(f, CredentialFilter) for f in filters) == 1

    def test_creating_a_client_installs_it(self, caplog):
        """End to end: the filter is in place once a session exists."""
        from enappsys import EnAppSys

        client = EnAppSys()
        try:
            logger = logging.getLogger("urllib3.connectionpool")
            assert any(isinstance(f, CredentialFilter) for f in logger.filters)

            with caplog.at_level(logging.DEBUG, logger="urllib3.connectionpool"):
                logger.debug("GET /csvapi?user=someone&pass=sensitive-value")
            assert "sensitive-value" not in caplog.text
            assert "pass=<redacted>" in caplog.text
            assert "user=someone" in caplog.text
        finally:
            client._session.session.close()
