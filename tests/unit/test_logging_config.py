import asyncio
import json
import logging

import globus_sdk
import pytest
from fastapi import HTTPException
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from stac_fastapi.globus_search import core, logging_config
from stac_fastapi.globus_search.logging_config import (
    RequestLoggingMiddleware,
    build_logging_config,
    request_id_var,
)


def _record(**extra):
    record = logging.LogRecord(
        "x.y", logging.INFO, __file__, 1, "hello %s", ("w",), None
    )
    record.__dict__.update(extra)
    return record


def _json_formatter(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    cfg = build_logging_config(log_format="json")["formatters"]["json"]
    cfg = dict(cfg)
    cls = cfg.pop("()")
    return cls(**cfg)


def test_json_format_has_expected_fields(monkeypatch):
    monkeypatch.setenv("APP_VERSION", "9.9.9")
    out = json.loads(
        _json_formatter(monkeypatch).format(_record(request_id="abc", extra_field=1))
    )
    assert out["message"] == "hello w"
    assert out["level"] == "INFO"
    assert out["logger"] == "x.y"
    assert out["service"] == "west-discovery"
    assert out["version"] == "9.9.9"
    assert out["request_id"] == "abc"
    assert out["extra_field"] == 1
    assert "timestamp" in out


def test_json_format_omits_request_id_outside_request(monkeypatch):
    out = json.loads(_json_formatter(monkeypatch).format(_record(request_id="-")))
    assert "request_id" not in out


def test_json_format_includes_traceback_in_one_line(monkeypatch):
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        record = _record(request_id="-")
        record.exc_info = sys.exc_info()
    line = _json_formatter(monkeypatch).format(record)
    assert "\n" not in line
    assert "RuntimeError" in json.loads(line)["exc_info"]


def test_config_defaults_and_overrides(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    cfg = build_logging_config()
    assert cfg["root"]["level"] == "INFO"
    assert cfg["handlers"]["stdout"]["formatter"] == "json"
    assert cfg["loggers"]["uvicorn.access"]["propagate"] is False

    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("LOG_FORMAT", "text")
    cfg = build_logging_config()
    assert cfg["root"]["level"] == "DEBUG"
    assert cfg["handlers"]["stdout"]["formatter"] == "text"


def test_config_unknown_format_falls_back_to_json():
    cfg = build_logging_config(log_format="yaml")
    assert cfg["handlers"]["stdout"]["formatter"] == "json"


class _Response:
    def __init__(self, status_code, headers):
        self.status_code = status_code
        self.headers = headers


class _Client:
    """Minimal ASGI driver (avoids needing httpx for tests)."""

    def __init__(self, endpoint):
        app = Starlette(routes=[Route("/ok", endpoint), Route("/boom", endpoint)])
        self.app = RequestLoggingMiddleware(app)

    def get(self, path, headers=None):
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [
                (k.lower().encode(), v.encode()) for k, v in (headers or {}).items()
            ],
            "client": ("203.0.113.9", 1234),
            "server": ("testserver", 80),
        }
        sent = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        async def run():
            try:
                await self.app(scope, receive, send)
            except Exception:  # noqa: BLE001 - mimic the server's 500
                return _Response(500, {})
            start = next(m for m in sent if m["type"] == "http.response.start")
            return _Response(
                start["status"],
                {k.decode(): v.decode() for k, v in start["headers"]},
            )

        return asyncio.run(run())


def _client(endpoint):
    return _Client(endpoint)


def _access(caplog):
    return [r for r in caplog.records if r.name == logging_config.access_logger.name]


def test_middleware_logs_request_and_sets_header(caplog):
    seen = {}

    async def ok(request):
        seen["id"] = request_id_var.get()
        return PlainTextResponse("hi")

    with caplog.at_level(logging.INFO, logger=logging_config.access_logger.name):
        resp = _client(ok).get("/ok", headers={"X-Request-ID": "req-123"})

    assert resp.headers["x-request-id"] == "req-123"
    assert seen["id"] == "req-123"
    assert request_id_var.get() is None
    (rec,) = _access(caplog)
    assert (rec.http_method, rec.path, rec.status) == ("GET", "/ok", 200)
    assert rec.duration_ms >= 0
    assert rec.levelno == logging.INFO


def test_middleware_generates_id_and_rejects_unsafe_inbound_id():
    async def ok(request):
        return PlainTextResponse("hi")

    resp = _client(ok).get("/ok", headers={"X-Request-ID": "bad id\twith space"})
    assert resp.headers["x-request-id"] != "bad id\twith space"
    assert len(resp.headers["x-request-id"]) == 32


def test_middleware_falls_back_to_alb_trace_id():
    async def ok(request):
        return PlainTextResponse("hi")

    trace = "Root=1-67891233-abcdef012345678912345678"
    resp = _client(ok).get("/ok", headers={"X-Amzn-Trace-Id": trace})
    assert resp.headers["x-request-id"] == trace


def test_middleware_logs_5xx_at_error_level(caplog):
    async def fail(request):
        return PlainTextResponse("no", status_code=503)

    with caplog.at_level(logging.INFO, logger=logging_config.access_logger.name):
        _client(fail).get("/ok")
    assert _access(caplog)[0].levelno == logging.ERROR


def test_middleware_logs_unhandled_exception(caplog):
    async def boom(request):
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.INFO, logger=logging_config.access_logger.name):
        resp = _client(boom).get("/boom")

    assert resp.status_code == 500
    (rec,) = _access(caplog)
    assert rec.status == 500
    assert rec.exc_info is not None
    assert request_id_var.get() is None


def test_request_id_filter_defaults_to_dash():
    record = _record()
    logging_config.RequestIdFilter().filter(record)
    assert record.request_id == "-"


@pytest.mark.parametrize(
    "status,expected,level",
    [
        (400, 400, logging.WARNING),
        (404, 404, logging.WARNING),
        (403, 403, logging.WARNING),
        (500, 502, logging.ERROR),
    ],
)
def test_globus_search_errors_are_logged_and_translated(
    caplog, mocker, status, expected, level
):
    err = mocker.Mock(spec=globus_sdk.SearchAPIError)
    err.http_status = status
    err.code = "SomeCode"
    err.message = "upstream said no"
    err.request_id = "globus-req-1"
    err.headers = {}

    with (
        caplog.at_level(logging.WARNING, logger=core.logger.name),
        pytest.raises(HTTPException) as exc_info,
    ):
        core._raise_for_search_error(err, "post_search")

    assert exc_info.value.status_code == expected
    (rec,) = [r for r in caplog.records if r.name == core.logger.name]
    assert rec.levelno == level
    assert rec.upstream_status == status
    assert rec.upstream_code == "SomeCode"
    assert rec.upstream_request_id == "globus-req-1"
    assert rec.operation == "post_search"
