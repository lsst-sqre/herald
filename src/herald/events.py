"""Metrics events for the Herald service."""

from typing import override

from pydantic import Field
from safir.dependencies.metrics import EventMaker
from safir.metrics import EventManager, EventPayload

__all__ = ["AlertRequestFailureEvent", "AlertRequestSuccessEvent", "Events"]


class AlertRequestSuccessEvent(EventPayload):
    """Successful alert-related request."""

    user: str = Field(..., title="Username")
    alert_id: int = Field(..., title="Alert ID")
    endpoint: str = Field(
        ..., title="Endpoint name (alert, cutouts, schema, links)"
    )
    fetch_duration_ms: float = Field(
        ..., title="Time spent fetching data in milliseconds"
    )
    processing_duration_ms: float = Field(
        ..., title="Time spent processing the result in milliseconds"
    )
    total_duration_ms: float = Field(
        ..., title="Total request duration in milliseconds"
    )


class AlertRequestFailureEvent(EventPayload):
    """Failed alert-related request."""

    user: str | None = Field(None, title="Username (if available)")
    alert_id: int | None = Field(None, title="Alert ID (if parseable)")
    endpoint: str = Field(
        ..., title="Endpoint name (alert, cutouts, schema, links)"
    )
    error_type: str = Field(..., title="Error type or exception class name")
    fetch_duration_ms: float = Field(
        ..., title="Time spent fetching data in milliseconds"
    )
    processing_duration_ms: float = Field(
        ..., title="Time spent processing the result in milliseconds"
    )
    total_duration_ms: float = Field(
        ..., title="Total request duration in milliseconds"
    )


class Events(EventMaker):
    """Event publishers for all Herald metrics."""

    @override
    async def initialize(self, manager: EventManager) -> None:
        self.alert_success = await manager.create_publisher(
            "alert_success", AlertRequestSuccessEvent
        )
        self.alert_failure = await manager.create_publisher(
            "alert_failure", AlertRequestFailureEvent
        )
