"""The main application factory for the herald service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from aiobotocore.session import get_session
from fastapi import FastAPI, Request
from fastapi.responses import Response
from safir.dependencies.http_client import http_client_dependency
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.slack.webhook import SlackRouteErrorHandler

from .config import config
from .dependencies.requestcontext import context_dependency
from .exceptions import (
    AlertNotFoundError,
    CorruptAlertError,
    SchemaNotFoundError,
)
from .handlers.external import external_router
from .handlers.internal import internal_router

__all__ = ["app"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Set up and tear down the application."""
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=config.s3_endpoint_url,
        region_name=config.s3_region,
        **config.s3_credentials,
    ) as s3_client:
        await context_dependency.initialize(s3_client)
        yield

    await context_dependency.aclose()
    await http_client_dependency.aclose()


configure_logging(
    profile=config.log_profile,
    log_level=config.log_level,
    name="herald",
)
configure_uvicorn_logging(config.log_level)

app = FastAPI(
    title="herald",
    description=metadata("herald")["Summary"],
    version=version("herald"),
    openapi_url=f"{config.path_prefix}/openapi.json",
    docs_url=f"{config.path_prefix}/docs",
    redoc_url=f"{config.path_prefix}/redoc",
    redirect_slashes=False,
    lifespan=lifespan,
)
"""The main FastAPI application for herald."""

app.include_router(internal_router)
app.include_router(external_router, prefix=f"{config.path_prefix}")
app.add_middleware(XForwardedMiddleware)


@app.exception_handler(AlertNotFoundError)
async def _alert_not_found(
    request: Request, exc: AlertNotFoundError
) -> Response:
    return Response(content=str(exc), media_type="text/plain", status_code=404)


@app.exception_handler(SchemaNotFoundError)
async def _schema_not_found(
    request: Request, exc: SchemaNotFoundError
) -> Response:
    return Response(content=str(exc), media_type="text/plain", status_code=404)


@app.exception_handler(CorruptAlertError)
async def _corrupt_alert(request: Request, exc: CorruptAlertError) -> Response:
    return Response(content=str(exc), media_type="text/plain", status_code=500)


if config.slack_webhook:
    logger = structlog.get_logger("herald")
    SlackRouteErrorHandler.initialize(config.slack_webhook, "herald", logger)
    logger.debug("Initialized Slack webhook")
