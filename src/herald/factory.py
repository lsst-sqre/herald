"""Factory for Herald services and process-wide context."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self

from structlog.stdlib import BoundLogger

from .config import config
from .services.alert import AlertService
from .storage import AlertStore

__all__ = ["Factory", "ProcessContext"]


@dataclass(kw_only=True, frozen=True, slots=True)
class ProcessContext:
    """Per-process application context.

    This object caches all of the per-process singletons that can be reused
    for every request and only need to be recreated if the application
    configuration changes.
    """

    alert_store: AlertStore
    """The S3-backed alert store."""

    @classmethod
    async def create(cls, s3_client: Any) -> Self:
        """Create a ``ProcessContext`` from an aiobotocore S3 client.

        Parameters
        ----------
        s3_client
            An initialised aiobotocore S3 client. Lifecycle (creation and
            closure) is managed by the caller (the app lifespan).
        """
        return cls(
            alert_store=AlertStore(s3_client=s3_client, config=config),
        )

    async def aclose(self) -> None:
        """Close any resources held by the context.

        The S3 client lifecycle is managed by the lifespan ``async with``
        block, so nothing needs to be closed here.
        """


class Factory:
    """Build Herald components.

    Uses the contents of a `ProcessContext` to construct the components of
    the application on demand.

    Parameters
    ----------
    process_context
        Shared process context.
    logger
        Logger to use for messages.
    """

    def __init__(
        self,
        *,
        process_context: ProcessContext,
        logger: BoundLogger,
    ) -> None:
        self._process_context = process_context
        self._logger = logger

    def set_logger(self, logger: BoundLogger) -> None:
        """Update the logger, propagating new context to future services.

        Parameters
        ----------
        logger
            The rebound logger to use for subsequent service creation.
        """
        self._logger = logger

    def create_alert_service(self) -> AlertService:
        """Create an ``AlertService`` for the current request."""
        return AlertService(
            store=self._process_context.alert_store,
            logger=self._logger,
        )
