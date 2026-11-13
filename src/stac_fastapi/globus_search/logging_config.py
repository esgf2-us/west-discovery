"""
Logging configuration and request logging middleware.

Logs are written to stdout, one record per line. In ECS with the ``awslogs``
driver this means each line becomes one CloudWatch event, and when the line is
JSON, CloudWatch Logs Insights discovers its fields automatically.

Environment variables:

``LOG_LEVEL``
    Root log level (default ``INFO``).
``LOG_FORMAT``
    ``json`` (default) or ``text`` (human readable, for local development).
``SERVICE_NAME``
    Value of the ``service`` field on every JSON record
    (default ``west-discovery``).
``APP_VERSION``
    Optional. Added as the ``version`` field on every JSON record if set.
"""

import logging
import logging.config
import os
import re
import time
import uuid
from contextvars import ContextVar
from typing import Any

from pythonjsonlogger.json import JsonFormatter

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

NO_REQUEST_ID = "-"
REQUEST_ID_HEADER = "X-Request-ID"
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:=;,\-]{1,128}$")

access_logger = logging.getLogger("stac_fastapi.globus_search.access")


class RequestIdFilter(logging.Filter):
    """Attach the current request ID (or ``-``) to every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get() or NO_REQUEST_ID
        return True


class AppJsonFormatter(JsonFormatter):
    """JSON formatter that omits ``request_id`` outside of a request."""

    def process_log_record(self, log_record: dict[str, Any]) -> dict[str, Any]:
        if log_record.get("request_id") == NO_REQUEST_ID:
            log_record.pop("request_id")
        return super().process_log_record(log_record)


def build_logging_config(
    level: str | None = None, log_format: str | None = None
) -> dict[str, Any]:
    """Build a ``logging.config.dictConfig`` dictionary from the environment."""
    level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    log_format = (log_format or os.getenv("LOG_FORMAT") or "json").lower()

    static_fields = {"service": os.getenv("SERVICE_NAME", "west-discovery")}
    if version := os.getenv("APP_VERSION"):
        static_fields["version"] = version

    formatters: dict[str, Any] = {
        "json": {
            "()": AppJsonFormatter,
            "fmt": "%(asctime)s %(levelname)s %(name)s %(request_id)s %(message)s",
            "rename_fields": {
                "asctime": "timestamp",
                "levelname": "level",
                "name": "logger",
            },
            "static_fields": static_fields,
        },
        "text": {
            "format": "%(asctime)s %(levelname)-8s %(name)s [%(request_id)s] %(message)s",
        },
    }
    if log_format not in formatters:
        log_format = "json"

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"request_id": {"()": RequestIdFilter}},
        "formatters": formatters,
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": log_format,
                "filters": ["request_id"],
            }
        },
        "root": {"level": level, "handlers": ["stdout"]},
        "loggers": {
            "uvicorn": {"level": level, "handlers": [], "propagate": True},
            "uvicorn.error": {"level": level, "handlers": [], "propagate": True},
            # The request middleware emits one structured line per request,
            # so uvicorn's own access log would be a duplicate.
            "uvicorn.access": {"handlers": [], "propagate": False},
            # hishel logs three INFO lines per request; keep warnings only.
            "hishel.asgi": {"level": "WARNING"},
        },
    }


def configure_logging(level: str | None = None, log_format: str | None = None) -> None:
    """Configure logging for the whole process. Safe to call repeatedly."""
    logging.config.dictConfig(build_logging_config(level, log_format))


def _header(scope: dict, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key == name:
            return value.decode("latin-1")
    return None


def _resolve_request_id(scope: dict) -> str:
    """Use a sane inbound ID (``X-Request-ID`` then ALB's trace ID) or make one."""
    for name in (b"x-request-id", b"x-amzn-trace-id"):
        candidate = _header(scope, name)
        if candidate and _SAFE_REQUEST_ID.match(candidate):
            return candidate
    return uuid.uuid4().hex


class RequestLoggingMiddleware:
    """
    Pure ASGI middleware that assigns a request ID, exposes it on the response
    and logs one line per HTTP request (method, path, status, duration).
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _resolve_request_id(scope)
        token = request_id_var.set(request_id)
        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = [
                    (k, v)
                    for k, v in message.get("headers", [])
                    if k.lower() != b"x-request-id"
                ]
                headers.append((b"x-request-id", request_id.encode("latin-1")))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            access_logger.exception(
                "Unhandled exception while processing request",
                extra=self._fields(scope, 500, start),
            )
            raise
        else:
            access_logger.log(
                logging.ERROR if status_code >= 500 else logging.INFO,
                "%s %s %s",
                scope["method"],
                scope["path"],
                status_code,
                extra=self._fields(scope, status_code, start),
            )
        finally:
            request_id_var.reset(token)

    @staticmethod
    def _fields(scope: dict, status_code: int, start: float) -> dict[str, Any]:
        client = scope.get("client")
        return {
            "http_method": scope["method"],
            "path": scope["path"],
            "status": status_code,
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "client_ip": client[0] if client else None,
            "user_agent": _header(scope, b"user-agent"),
        }
