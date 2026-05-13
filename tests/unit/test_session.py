from __future__ import annotations

import logging
from json import JSONDecodeError

import pytest
import requests

from enappsys.exceptions import HTTPError
from enappsys.session import Session


class FakeResponse:
    status_code = 200
    headers = {"Content-Type": "text/plain"}
    text = "ok"


class InvalidJSONResponse:
    status_code = 200
    headers = {"Content-Type": "application/json"}

    def json(self):
        raise JSONDecodeError("Expecting value", "", 0)


def test_session_get_logs_sanitized_request_params(monkeypatch, caplog):
    session = Session(
        user="test-user",
        secret="test-secret",
        credentials_file=None,
        max_retries=0,
        agent_id=None,
    )

    def fake_get(url, params):
        assert url.endswith("/csvapi")
        assert params["user"] == "test-user"
        assert params["pass"] == "test-secret"
        return FakeResponse()

    monkeypatch.setattr(session.session, "get", fake_get)

    with caplog.at_level(logging.DEBUG, logger="enappsys.session"):
        response = session.get(
            "csvapi",
            {
                "type": "ENTSOE_DAY_AHEAD_PRICES",
                "entities": ["BE.BELGIUM"],
            },
        )

    assert response == "ok"
    assert "GET" in caplog.text
    assert "csvapi" in caplog.text
    assert "ENTSOE_DAY_AHEAD_PRICES" in caplog.text
    assert "BE.BELGIUM" in caplog.text
    assert "test-user" not in caplog.text
    assert "test-secret" not in caplog.text


def test_session_get_sanitizes_request_exception(monkeypatch):
    session = Session(
        user="test-user",
        secret="test-secret",
        credentials_file=None,
        max_retries=0,
        agent_id=None,
    )

    def fake_get(url, params):
        raise requests.ConnectionError(
            "failed url with user=test-user and pass=test-secret"
        )

    monkeypatch.setattr(session.session, "get", fake_get)

    with pytest.raises(HTTPError) as exc_info:
        session.get("csvapi", {"type": "ENTSOE_DAY_AHEAD_PRICES"})

    message = str(exc_info.value)
    assert "ConnectionError" in message
    assert "csvapi" in message
    assert "ENTSOE_DAY_AHEAD_PRICES" in message
    assert "test-user" not in message
    assert "test-secret" not in message


def test_session_get_adds_request_context_to_json_parse_errors(monkeypatch):
    session = Session(
        user="test-user",
        secret="test-secret",
        credentials_file=None,
        max_retries=0,
        agent_id=None,
    )

    def fake_get(url, params):
        return InvalidJSONResponse()

    monkeypatch.setattr(session.session, "get", fake_get)

    with pytest.raises(HTTPError) as exc_info:
        session.get("jsonapi", {"type": "ENTSOE_DAY_AHEAD_PRICES"})

    message = str(exc_info.value)
    assert "Failed to parse JSON response" in message
    assert "jsonapi" in message
    assert "ENTSOE_DAY_AHEAD_PRICES" in message
    assert "test-user" not in message
    assert "test-secret" not in message
