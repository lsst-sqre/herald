"""Internal HTTP handlers that serve relative to the root path, ``/``.

These handlers aren't externally visible since the app is available at a path
prefix (e.g. ``/api/alerts``). See `herald.handlers.external` for
the external endpoint handlers.

These handlers should be used for monitoring, health checks, internal status,
or other information that should not be visible outside the Kubernetes cluster.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from pyinstrument import Profiler
from safir.metadata import Metadata, get_metadata
from safir.slack.webhook import SlackRouteErrorHandler

from ..config import config
from ..dependencies import RequestContext, context_dependency

__all__ = ["internal_router"]

internal_router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all internal handlers."""


@internal_router.get(
    "/",
    description=(
        "Return metadata about the running application. Can also be used as"
        " a health check. This route is not exposed outside the cluster and"
        " therefore cannot be used by external clients."
    ),
    include_in_schema=False,
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Metadata:
    return get_metadata(
        package_name="herald",
        application_name=config.name,
    )


@internal_router.get(
    "/profile",
    description=(
        "Fetch an alert while profiling and return an HTML flamegraph."
        " Only accessible within the cluster."
    ),
    include_in_schema=False,
    summary="CPU profile",
)
async def get_profile(
    id: Annotated[
        int, Query(description="Alert ID to fetch while profiling.")
    ],
    context: Annotated[RequestContext, Depends(context_dependency)],
    responseformat: Annotated[
        str, Query(description="Format to profile: avro, fits, or json.")
    ] = "avro",
) -> HTMLResponse:
    profiler = Profiler(async_mode="enabled")
    profiler.start()
    alert_service = context.factory.create_alert_service()
    if responseformat == "fits":
        await alert_service.get_alert_fits(id)
    elif responseformat == "json":
        await alert_service.get_alert_json(id)
    else:
        await alert_service.get_alert_avro(id)
    profiler.stop()
    return HTMLResponse(profiler.output_html())


@internal_router.get(
    "/profile/fits-cpu",
    description=(
        "Profile the FITS conversion CPU work for a single alert and return"
        " a cProfile stats report. Only accessible within the cluster."
    ),
    include_in_schema=False,
    summary="FITS conversion CPU profile",
)
async def get_fits_cpu_profile(
    id: Annotated[
        int, Query(description="Alert ID to fetch while profiling.")
    ],
    context: Annotated[RequestContext, Depends(context_dependency)],
    sort: Annotated[
        str, Query(description="Sort by: cumulative, tottime, calls.")
    ] = "cumulative",
) -> PlainTextResponse:
    alert_service = context.factory.create_alert_service()
    stats = await alert_service.profile_fits_conversion(id, sort=sort)
    return PlainTextResponse(stats)
