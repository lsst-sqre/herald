"""Per-request context and its FastAPI dependency."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, Request
from safir.dependencies.logger import logger_dependency
from structlog.stdlib import BoundLogger

from ..factory import Factory, ProcessContext

__all__ = ["ContextDependency", "RequestContext", "context_dependency"]


@dataclass(slots=True)
class RequestContext:
    """Holds the incoming request and its surrounding context.

    The primary reason for the existence of this class is to allow the
    functions involved in request processing to repeatedly rebind the request
    logger to include more information, without having to pass both the
    request and the logger separately to every function.
    """

    request: Request
    """The incoming request."""

    logger: BoundLogger
    """The request logger, optionally rebound with additional context."""

    factory: Factory
    """Factory for creating per-request service instances."""

    def rebind_logger(self, **values: str | None) -> None:
        """Add the given values to the logging context.

        Updates both the context logger and the factory's logger.

        Parameters
        ----------
        **values
            Additional key/value pairs to bind to the logger.
        """
        self.logger = self.logger.bind(**values)
        self.factory.set_logger(self.logger)


class ContextDependency:
    """Provide a per-request context as a FastAPI dependency.

    Each request gets a `RequestContext`. To save overhead, the portions of
    the context that are shared by all requests are collected into the
    single process-global `~herald.factory.ProcessContext` and reused with
    each request.
    """

    def __init__(self) -> None:
        self._process_context: ProcessContext | None = None

    async def __call__(
        self,
        request: Request,
        logger: Annotated[BoundLogger, Depends(logger_dependency)],
    ) -> RequestContext:
        """Create and return a per-request context."""
        return RequestContext(
            request=request,
            logger=logger,
            factory=self._create_factory(logger),
        )

    @property
    def process_context(self) -> ProcessContext:
        """The process-wide context shared across all requests."""
        if self._process_context is None:
            raise RuntimeError("ContextDependency has not been initialized")
        return self._process_context

    async def initialize(self, s3_client: Any) -> None:
        """Initialize the process-wide context.

        Parameters
        ----------
        s3_client
            An initialised aiobotocore S3 client whose lifecycle is managed
            by the app lifespan.
        """
        if self._process_context is not None:
            await self._process_context.aclose()
        self._process_context = await ProcessContext.create(s3_client)

    async def aclose(self) -> None:
        """Release the process-wide context on shutdown."""
        if self._process_context is not None:
            await self._process_context.aclose()
        self._process_context = None

    def _create_factory(self, logger: BoundLogger) -> Factory:
        return Factory(
            process_context=self.process_context,
            logger=logger,
        )


context_dependency = ContextDependency()
"""Singleton dependency that returns a per-request ``RequestContext``."""
