"""Test fixtures for herald tests."""

# Set required env vars before any herald imports so Config() succeeds.
import os

os.environ.setdefault("HERALD_S3_ALERTS_BUCKET", "test-alerts-bucket")
os.environ.setdefault("HERALD_S3_SCHEMAS_BUCKET", "test-schemas-bucket")

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from safir.dependencies.gafaelfawr import auth_dependency
from structlog.stdlib import BoundLogger

from herald import main
from herald.dependencies import RequestContext, context_dependency
from herald.factory import Factory
from herald.services.alert import AlertService
from herald.storage import AlertStore


@pytest_asyncio.fixture
async def app() -> AsyncGenerator[FastAPI]:
    """Return a configured test application with lifespan."""
    async with LifespanManager(main.app):
        yield main.app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        base_url="https://example.com/", transport=ASGITransport(app=app)
    ) as client:
        yield client


@pytest.fixture
def mock_store() -> MagicMock:
    """Return a mock ``AlertStore`` with async methods."""
    store = MagicMock(spec=AlertStore)
    store.get_alert_bytes = AsyncMock()
    store.get_schema_bytes = AsyncMock()
    return store


@pytest_asyncio.fixture
async def client_with_mock_store(
    mock_store: MagicMock,
) -> AsyncGenerator[AsyncClient]:
    """Return a test client with a mock storage backend.

    Uses FastAPI dependency overrides to inject a mock ``AlertStore`` and
    bypass Gafaelfawr auth. No real S3 connection is required.
    """

    async def override_context(request: Request) -> RequestContext:
        mock_factory = MagicMock(spec=Factory)
        mock_factory.create_alert_service.return_value = AlertService(
            store=mock_store,
            logger=MagicMock(spec=BoundLogger),
        )
        return RequestContext(
            request=request,
            logger=MagicMock(spec=BoundLogger),
            factory=mock_factory,
        )

    main.app.dependency_overrides[context_dependency] = override_context
    main.app.dependency_overrides[auth_dependency] = lambda: "test-user"

    async with AsyncClient(
        base_url="https://example.com/",
        transport=ASGITransport(app=main.app),
    ) as client:
        yield client

    main.app.dependency_overrides.clear()
