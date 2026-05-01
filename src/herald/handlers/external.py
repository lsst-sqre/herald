"""Handlers for the app's external root, ``/herald/``."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, Response
from safir.dependencies.gafaelfawr import auth_dependency
from safir.metadata import get_metadata
from safir.slack.webhook import SlackRouteErrorHandler

from ..config import config
from ..constants import IAU_ALERT_PREFIX
from ..dependencies import RequestContext, context_dependency
from ..events import AlertRequestFailureEvent, AlertRequestSuccessEvent
from ..models import Index
from .datalink import build_links_votable

__all__ = ["external_router"]

external_router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all external handlers."""

_AVRO_OCF_CONTENT_TYPE = "application/x-avro-ocf"
_FITS_CONTENT_TYPE = "application/fits"
_VOTABLE_CONTENT_TYPE = "application/x-votable+xml"
_FITS_FORMATS: frozenset[str] = frozenset({"fits", "application/fits"})
_JSON_FORMATS: frozenset[str] = frozenset({"json", "application/json"})
_ACCEPTED_FORMATS: frozenset[str] = _FITS_FORMATS | _JSON_FORMATS
_ALERT_PARAMS: frozenset[str] = frozenset({"ID", "RESPONSEFORMAT"})
_ID_ONLY_PARAMS: frozenset[str] = frozenset({"ID"})


def _check_unknown_params(
    request: Request, allowed: frozenset[str]
) -> Response | None:
    """Return a 400 Response if any unknown query params are present.

    Parameters
    ----------
    request
        The incoming request.
    allowed
        The set of allowed query parameter names.

    Returns
    -------
    Response | None
        A 400 Response if unknown params are found, or ``None`` otherwise.
    """
    unknown = [k for k in request.query_params if k not in allowed]
    if unknown:
        query_string = "?" + str(request.url.query)
        return Response(
            content=(
                f"Invalid parameter '{unknown[0]}'"
                f" in query string '{query_string}'"
            ),
            media_type="text/plain",
            status_code=400,
        )
    return None


def _parse_alert_id(alert_id: str) -> int:
    """Parse a bare integer or IAU-format alert ID to an integer.

    Parameters
    ----------
    alert_id
        Either a decimal integer ID or an ``LSST-AP-DS-{n}`` string.

    Returns
    -------
    int
        The (numeric) alert ID.

    Raises
    ------
    ValueError
        With a DALI-compliant message for malformed or non-LSST IDs.
    """
    if alert_id.startswith("LSST-"):
        if not alert_id.startswith(IAU_ALERT_PREFIX):
            raise ValueError(f"Non-LSST alert ID '{alert_id}'")
        numeric = alert_id[len(IAU_ALERT_PREFIX) :]
    else:
        numeric = alert_id

    try:
        return int(numeric)
    except ValueError as e:
        raise ValueError(f"Invalid alert ID format in '{alert_id}'") from e


def _validate_responseformat(responseformat: str | None) -> Response | None:
    """Return a 415 plain-text Response if the format is not expected.

    Returns ``None`` when ``responseformat`` is acceptable.

    Parameters
    ----------
    responseformat
        The value of the ``RESPONSEFORMAT`` query parameter, or ``None`` if
        not provided.

    Returns
    -------
    Response | None
        A 415 Response if the format is invalid, or ``None`` if it is valid
        or not provided.
    """
    if responseformat is not None and responseformat not in _ACCEPTED_FORMATS:
        accepted = ", ".join(sorted(_ACCEPTED_FORMATS))
        return Response(
            content=(
                f"Invalid response format '{responseformat}';"
                f" accepted formats are {accepted}"
            ),
            media_type="text/plain",
            status_code=415,
        )
    return None


def _fits_attachment(content: bytes, filename: str) -> Response:
    """Return a FITS file as an attachment response.

    Parameters
    ----------
    content
        The content of the FITS file as bytes.
    filename
        The filename to suggest in the Content-Disposition header.

    Returns
    -------
    Response
        A Response object with the FITS content and appropriate headers.
    """
    return Response(
        content=content,
        media_type=_FITS_CONTENT_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _avro_attachment(content: bytes, alert_id: int) -> Response:
    """Return an Avro OCF file as an attachment response.

    Parameters
    ----------
    content
        The content of the Avro OCF file as bytes.
    alert_id
        The alert ID, used to construct the filename in the Content-Disposition
        header.

    Returns
    -------
    Response
        A Response object with the Avro OCF content and appropriate headers.
    """
    return Response(
        content=content,
        media_type=_AVRO_OCF_CONTENT_TYPE,
        headers={
            "Content-Disposition": (
                f'attachment; filename="alert-{alert_id}.avro"'
            )
        },
    )


@external_router.get(
    "/links",
    summary="DataLink links for an alert",
    description=(
        "Return a DataLink VOTable listing all available related data "
        "products for the given alert ID."
    ),
)
async def get_alert_links(
    request: Request,
    id: Annotated[str, Query(alias="ID", description="Alert ID.")],
    user: Annotated[str, Depends(auth_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Response:
    if error := _check_unknown_params(request, _ID_ONLY_PARAMS):
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="links",
                error_type="unknown_params",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return error
    try:
        alert_id = _parse_alert_id(id)
    except ValueError as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="links",
                error_type="invalid_alert_id",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return Response(
            content=str(e), media_type="text/plain", status_code=400
        )
    context.rebind_logger(user=user, alert_id=str(alert_id))
    context.logger.info("DataLink links request")
    service_base_url = (
        str(request.url).partition("?")[0].removesuffix("/links")
    )
    await context.factory.events.alert_success.publish(
        AlertRequestSuccessEvent(
            user=user,
            alert_id=alert_id,
            endpoint="links",
            fetch_duration_ms=0.0,
            total_duration_ms=0.0,
            processing_duration_ms=0.0,
        )
    )
    return Response(
        content=build_links_votable(alert_id, service_base_url),
        media_type=_VOTABLE_CONTENT_TYPE,
    )


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
    "/cutouts",
    summary="Get cutout images for an alert",
    description=(
        "Return the cutout postage stamp images for the given alert as a "
        "FITS file. Includes the difference, science and template cutouts."
    ),
)
async def get_alert_cutouts(
    request: Request,
    id: Annotated[str, Query(alias="ID", description="Alert ID.")],
    user: Annotated[str, Depends(auth_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Response:
    if error := _check_unknown_params(request, _ID_ONLY_PARAMS):
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="cutouts",
                error_type="unknown_params",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return error
    try:
        alert_id = _parse_alert_id(id)
    except ValueError as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="cutouts",
                error_type="invalid_alert_id",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return Response(
            content=str(e), media_type="text/plain", status_code=400
        )
    context.rebind_logger(user=user, alert_id=str(alert_id))
    context.logger.info("Alert cutouts request")
    alert_service = context.factory.create_alert_service()
    try:
        content = await alert_service.get_alert_cutouts(alert_id)
    except Exception as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                alert_id=alert_id,
                endpoint="cutouts",
                error_type=type(e).__name__,
                fetch_duration_ms=alert_service.fetch_duration_ms,
                total_duration_ms=alert_service.total_duration_ms,
                processing_duration_ms=alert_service.processing_duration_ms,
            )
        )
        raise
    await context.factory.events.alert_success.publish(
        AlertRequestSuccessEvent(
            user=user,
            alert_id=alert_id,
            endpoint="cutouts",
            fetch_duration_ms=alert_service.fetch_duration_ms,
            total_duration_ms=alert_service.total_duration_ms,
            processing_duration_ms=alert_service.processing_duration_ms,
        )
    )
    return _fits_attachment(content, f"cutouts-{alert_id}.fits")


@external_router.get(
    "/schema",
    summary="Get Avro schema for an alert",
    description=(
        "Return the Avro schema used to serialise the given alert. The schema"
        " is identified by the schema ID embedded in the alert's Confluent"
        " wire format header."
    ),
)
async def get_alert_schema(
    request: Request,
    id: Annotated[str, Query(alias="ID", description="Alert ID.")],
    user: Annotated[str, Depends(auth_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Response:
    if error := _check_unknown_params(request, _ID_ONLY_PARAMS):
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="schema",
                error_type="unknown_params",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return error
    try:
        alert_id = _parse_alert_id(id)
    except ValueError as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="schema",
                error_type="invalid_alert_id",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return Response(
            content=str(e), media_type="text/plain", status_code=400
        )
    context.rebind_logger(user=user, alert_id=str(alert_id))
    context.logger.info("Alert schema request")
    alert_service = context.factory.create_alert_service()
    try:
        schema = await alert_service.get_alert_schema(alert_id)
    except Exception as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                alert_id=alert_id,
                endpoint="schema",
                error_type=type(e).__name__,
                fetch_duration_ms=alert_service.fetch_duration_ms,
                total_duration_ms=alert_service.total_duration_ms,
                processing_duration_ms=alert_service.processing_duration_ms,
            )
        )
        raise
    await context.factory.events.alert_success.publish(
        AlertRequestSuccessEvent(
            user=user,
            alert_id=alert_id,
            endpoint="schema",
            fetch_duration_ms=alert_service.fetch_duration_ms,
            total_duration_ms=alert_service.total_duration_ms,
            processing_duration_ms=alert_service.processing_duration_ms,
        )
    )
    return JSONResponse(content=schema)


@external_router.get(
    "",
    summary="Get alert packet",
    description=(
        "Retrieve an alert packet by its ID. Accepts either an integer "
        "alert ID or the IAU form ``LSST-AP-DS-{n}`` via the ``ID`` query "
        "parameter. "
        "By default returns an Avro OCF container file with the schema "
        "embedded (``application/x-avro-ocf``). "
        "Use ``RESPONSEFORMAT=fits`` or ``RESPONSEFORMAT=application/fits`` "
        "to receive a multi-extension FITS file. "
        "Use ``RESPONSEFORMAT=json`` or ``RESPONSEFORMAT=application/json`` "
        "to receive the alert as JSON."
    ),
)
async def get_alert(
    request: Request,
    id: Annotated[str, Query(alias="ID", description="Alert ID.")],
    user: Annotated[str, Depends(auth_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
    responseformat: Annotated[
        str | None,
        Query(alias="RESPONSEFORMAT", description="Response format."),
    ] = None,
) -> Response:
    if error := _check_unknown_params(request, _ALERT_PARAMS):
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="alert",
                error_type="unknown_params",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return error
    try:
        numeric_id = _parse_alert_id(alert_id=id)
    except ValueError as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                endpoint="alert",
                error_type="invalid_alert_id",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return Response(
            content=str(e), media_type="text/plain", status_code=400
        )
    context.rebind_logger(user=user, alert_id=str(numeric_id))
    context.logger.info("Alert retrieval request")

    if error_response := _validate_responseformat(responseformat):
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                alert_id=numeric_id,
                endpoint="alert",
                error_type="unsupported_format",
                fetch_duration_ms=0.0,
                total_duration_ms=0.0,
                processing_duration_ms=0.0,
            )
        )
        return error_response

    alert_service = context.factory.create_alert_service()

    try:
        if responseformat in _FITS_FORMATS:
            content = await alert_service.get_alert_fits(numeric_id)
            await context.factory.events.alert_success.publish(
                AlertRequestSuccessEvent(
                    user=user,
                    alert_id=numeric_id,
                    endpoint="alert",
                    fetch_duration_ms=alert_service.fetch_duration_ms,
                    total_duration_ms=alert_service.total_duration_ms,
                    processing_duration_ms=alert_service.processing_duration_ms,
                )
            )
            return _fits_attachment(content, f"alert-{numeric_id}.fits")

        if responseformat in _JSON_FORMATS:
            content_json = await alert_service.get_alert_json(numeric_id)
            await context.factory.events.alert_success.publish(
                AlertRequestSuccessEvent(
                    user=user,
                    alert_id=numeric_id,
                    endpoint="alert",
                    fetch_duration_ms=alert_service.fetch_duration_ms,
                    total_duration_ms=alert_service.total_duration_ms,
                    processing_duration_ms=alert_service.processing_duration_ms,
                )
            )
            return JSONResponse(content=content_json)

        # Default to Avro OCF if no format specified.
        content_avro = await alert_service.get_alert_avro(numeric_id)
        await context.factory.events.alert_success.publish(
            AlertRequestSuccessEvent(
                user=user,
                alert_id=numeric_id,
                endpoint="alert",
                fetch_duration_ms=alert_service.fetch_duration_ms,
                total_duration_ms=alert_service.total_duration_ms,
                processing_duration_ms=alert_service.processing_duration_ms,
            )
        )
        return _avro_attachment(content_avro, numeric_id)
    except Exception as e:
        await context.factory.events.alert_failure.publish(
            AlertRequestFailureEvent(
                user=user,
                alert_id=numeric_id,
                endpoint="alert",
                error_type=type(e).__name__,
                fetch_duration_ms=alert_service.fetch_duration_ms,
                total_duration_ms=alert_service.total_duration_ms,
                processing_duration_ms=alert_service.processing_duration_ms,
            )
        )
        raise
