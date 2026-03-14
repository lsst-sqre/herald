"""Handlers for the app's external root, ``/herald/``."""

import base64
import math
from collections.abc import Generator
from contextlib import contextmanager
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from safir.dependencies.gafaelfawr import auth_dependency
from safir.metadata import get_metadata
from safir.slack.webhook import SlackRouteErrorHandler

from ..config import config
from ..dependencies import RequestContext, context_dependency
from ..exceptions import AlertNotFoundError, SchemaNotFoundError
from ..models import Index

__all__ = ["external_router"]

external_router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all external handlers."""

_AVRO_CONTENT_TYPE = "application/avro"


def _none_if_nonfinite(f: float) -> float | None:
    """Return None for NaN/Inf floats, which are not valid JSON."""
    return None if math.isnan(f) or math.isinf(f) else f


@contextmanager
def _handle_alert_errors() -> Generator[None]:
    """Translate domain exceptions to HTTP responses."""
    try:
        yield
    except AlertNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(e)
        ) from e
    except SchemaNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(e)
        ) from e
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e)
        ) from e


@external_router.get(
    "/",
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index(
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Index:
    context.logger.info("Request for application metadata")
    return Index(
        metadata=get_metadata(
            package_name="herald",
            application_name=config.name,
        )
    )


@external_router.get(
    "/alerts/{alert_id}",
    summary="Get alert packet",
    description=(
        "Retrieve an alert packet by its ID. "
        "By default returns the deserialised record as JSON, with binary "
        "fields (e.g. cutout image stamps) base64-encoded. "
        "Send ``Accept: application/avro`` to receive an Avro OCF container "
        "file with the schema embedded."
    ),
)
async def get_alert(
    alert_id: int,
    user: Annotated[str, Depends(auth_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Response:
    context.rebind_logger(user=user, alert_id=str(alert_id))
    context.logger.info("Alert retrieval request")
    alert_service = context.factory.create_alert_service()

    accept = context.request.headers.get("accept", "application/json")
    want_avro = _AVRO_CONTENT_TYPE in accept

    with _handle_alert_errors():
        if want_avro:
            avro_bytes = await alert_service.get_alert_avro(alert_id)
            return Response(content=avro_bytes, media_type=_AVRO_CONTENT_TYPE)
        else:
            alert = await alert_service.get_alert(alert_id)
            content = jsonable_encoder(
                alert,
                custom_encoder={
                    bytes: lambda b: base64.b64encode(b).decode(),
                    float: _none_if_nonfinite,
                },
            )
            return JSONResponse(content=content)


@external_router.get(
    "/alerts/{alert_id}/schema",
    summary="Get Avro schema for an alert",
    description=(
        "Return the Avro schema used to serialise the given alert. The schema"
        " is identified by the schema ID embedded in the alert's Confluent"
        " wire format header."
    ),
    response_class=JSONResponse,
)
async def get_alert_schema(
    alert_id: int,
    user: Annotated[str, Depends(auth_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> dict[str, Any]:
    context.rebind_logger(user=user, alert_id=str(alert_id))
    context.logger.info("Alert schema request")
    alert_service = context.factory.create_alert_service()
    with _handle_alert_errors():
        return await alert_service.get_alert_schema(alert_id)
