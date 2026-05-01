"""Alert retrieval and deserialisation service."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import math
import struct
import time
from dataclasses import dataclass
from typing import Any

import fastavro
from structlog.stdlib import BoundLogger

from ..exceptions import CorruptAlertError
from ..storage import AlertStore
from .fits import alert_to_fits, cutouts_to_fits

__all__ = ["AlertService"]

# Confluent wire format magic byte
_CONFLUENT_MAGIC = 0x00
_HEADER_SIZE = 5  # 1 magic byte + 4 byte schema ID


@dataclass
class _ParsedAlert:
    """Internal container for a fetched and deserialised alert."""

    schema_dict: dict[str, Any]
    """Raw Avro schema as a Python dict, used for FITS column derivation."""

    parsed_schema: Any
    """Fastavro-compiled schema, used for Avro OCF serialisation."""

    record: dict[str, Any]
    """Deserialised alert record."""

    fetch_duration_ms: float
    """Time spent waiting for S3 responses in milliseconds."""

    processing_duration_ms: float
    """Time spent on CPU processing (deserialization etc.) in milliseconds."""


def _sanitize_for_json(obj: Any) -> Any:
    """Recursively prepare a deserialised Avro record for JSON serialisation.

    Handles two cases that are valid in Avro but not in JSON:
    - ``bytes`` fields (e.g. cutouts) are base64-encoded to ASCII strings.
    - ``Nan`` and ``inf`` are converted to ``None`` (JSON null).

    Parameters
    ----------
    obj
        A deserialised Avro record or any nested value within it.

    Returns
    -------
    Any
        The input with bytes base64-encoded and non-finite floats nulled.
    """
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(item) for item in obj]
    return obj


def _parse_confluent_header(alert_id: int, raw: bytes) -> tuple[int, bytes]:
    """Parse the 5-byte Confluent wire format header.

    Parameters
    ----------
    alert_id
        Used only for error messages.
    raw
        Decompressed alert bytes.

    Returns
    -------
    tuple[int, bytes]
        ``(schema_id, avro_payload)`` where ``schema_id`` is the 4-byte
        integer and ``avro_payload`` is everything after the header.

    Raises
    ------
    CorruptAlertError
        If the bytes are shorter than the 5-byte header, or if the magic
        byte is not the expected Confluent value.
    """
    if len(raw) < _HEADER_SIZE:
        raise CorruptAlertError(
            alert_id,
            f"stored bytes ({len(raw)}) are too short to contain a"
            f" Confluent wire format header ({_HEADER_SIZE} bytes required)",
        )

    magic = raw[0]
    if magic != _CONFLUENT_MAGIC:
        raise CorruptAlertError(
            alert_id,
            f"unexpected magic byte {magic:#04x},"
            f" expected {_CONFLUENT_MAGIC:#04x}",
        )

    (schema_id,) = struct.unpack(">I", raw[1:_HEADER_SIZE])
    avro_payload = raw[_HEADER_SIZE:]
    return schema_id, avro_payload


class AlertService:
    """Fetch and deserialise alert packets from the archive.

    Parameters
    ----------
    store
        The storage layer used to retrieve raw alert and schema bytes.
    logger
        Bound logger for this request.
    """

    def __init__(self, *, store: AlertStore, logger: BoundLogger) -> None:
        self._store = store
        self._logger = logger
        self.fetch_duration_ms: float = 0.0
        self.processing_duration_ms: float = 0.0
        self.total_duration_ms: float = 0.0
        self._request_start = time.monotonic()

    async def get_alert_avro(self, alert_id: int) -> bytes:
        """Retrieve an alert packet as an Avro OCF container file.

        Returns an Avro OCF with the schema embedded, so the response is
        self-describing and can be read by any Avro library without a
        separate schema request.

        Parameters
        ----------
        alert_id
            Alert ID (currently equal to diaSourceId).

        Returns
        -------
        bytes
            Avro OCF bytes containing the alert record and its schema.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        CorruptAlertError
            If the stored bytes do not start with the expected Confluent magic
            byte.
        """
        fetched = await self._fetch_record(alert_id)

        def _write() -> bytes:
            buf = io.BytesIO()
            fastavro.writer(buf, fetched.parsed_schema, [fetched.record])
            return buf.getvalue()

        proc_start = time.monotonic()
        result = await asyncio.to_thread(_write)
        self.processing_duration_ms += (time.monotonic() - proc_start) * 1000
        self.total_duration_ms = (
            time.monotonic() - self._request_start
        ) * 1000
        return result

    async def get_alert_json(self, alert_id: int) -> dict[str, Any]:
        """Retrieve an alert packet as a JSON-serialisable dict.

        Bytes fields (e.g. cutout stamps) are base64-encoded so the full
        alert is preserved in the response.

        Parameters
        ----------
        alert_id
            Alert ID (currently equal to diaSourceId).

        Returns
        -------
        dict
            Alert record with bytes fields base64-encoded as ASCII strings.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        CorruptAlertError
            If the stored bytes do not start with the expected Confluent magic
            byte.
        """
        fetched = await self._fetch_record(alert_id)
        proc_start = time.monotonic()
        result = _sanitize_for_json(fetched.record)
        self.processing_duration_ms += (time.monotonic() - proc_start) * 1000
        self.total_duration_ms = (
            time.monotonic() - self._request_start
        ) * 1000
        return result

    async def get_alert_schema(self, alert_id: int) -> dict[str, Any]:
        """Return the Avro schema for a given alert.

        Parameters
        ----------
        alert_id
            Alert ID used to locate the alert and extract its schema ID.

        Returns
        -------
        dict
            Parsed Avro schema as a Python dict.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        """
        s3_start = time.monotonic()
        raw = await self._store.get_alert_bytes(alert_id)
        s3_alert_done = time.monotonic()

        schema_id, _ = _parse_confluent_header(alert_id, raw)

        s3_schema_start = time.monotonic()
        schema_bytes = await self._store.get_schema_bytes(schema_id)
        s3_schema_done = time.monotonic()

        self.fetch_duration_ms = (
            (s3_alert_done - s3_start) + (s3_schema_done - s3_schema_start)
        ) * 1000

        proc_start = time.monotonic()
        result = json.loads(schema_bytes)
        self.processing_duration_ms = (time.monotonic() - proc_start) * 1000
        self.total_duration_ms = (
            time.monotonic() - self._request_start
        ) * 1000
        return result

    async def get_alert_cutouts(self, alert_id: int) -> bytes:
        """Retrieve cutout stamp images for an alert as a FITS file.

        Returns a FITS file containing one image per cutout stamp (difference,
        science, template). Each cutout is stored in the alert record as raw
        FITS bytes which are extracted and assembled into a single response.

        Parameters
        ----------
        alert_id
            Alert ID.

        Returns
        -------
        bytes
            FITS file containing the cutout image extensions.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        ValueError
            If the alert contains no cutout images, or if the Confluent
            header is malformed.
        """
        fetched = await self._fetch_record(alert_id)
        proc_start = time.monotonic()
        result = await asyncio.to_thread(cutouts_to_fits, fetched.record)
        self.processing_duration_ms += (time.monotonic() - proc_start) * 1000
        self.total_duration_ms = (
            time.monotonic() - self._request_start
        ) * 1000
        self._logger.debug("Assembled cutout FITS", alert_id=alert_id)
        return result

    async def get_alert_fits(self, alert_id: int) -> bytes:
        """Retrieve an alert as a full multi-extension FITS file.

        Assembles a FITS file containing image HDUs for each cutout stamp and
        BinTableHDUs for the alert, prior sources, forced photometry,
        DIA object, solar-system source, and MPC orbit data.

        Parameters
        ----------
        alert_id
            Alert ID.

        Returns
        -------
        bytes
            FITS file bytes.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        ValueError
            If the Confluent header is malformed.
        """
        fetched = await self._fetch_record(alert_id)
        proc_start = time.monotonic()
        result = await asyncio.to_thread(
            alert_to_fits, fetched.schema_dict, fetched.record
        )
        self.processing_duration_ms += (time.monotonic() - proc_start) * 1000
        self.total_duration_ms = (
            time.monotonic() - self._request_start
        ) * 1000
        self._logger.debug("Assembled full FITS", alert_id=alert_id)
        return result

    async def _fetch_record(self, alert_id: int) -> _ParsedAlert:
        """Fetch an alert from S3 and deserialise it.

        Parameters
        ----------
        alert_id
            Alert ID (currently equal to diaSourceId).

        Returns
        -------
        _ParsedAlert
            The fetched and deserialised alert.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        CorruptAlertError
            If the stored bytes do not start with the expected Confluent magic
            byte.
        """
        s3_start = time.monotonic()
        raw = await self._store.get_alert_bytes(alert_id)
        s3_alert_done = time.monotonic()

        schema_id, avro_payload = _parse_confluent_header(alert_id, raw)

        s3_schema_start = time.monotonic()
        schema_bytes = await self._store.get_schema_bytes(schema_id)
        s3_schema_done = time.monotonic()

        self.fetch_duration_ms = (
            (s3_alert_done - s3_start) + (s3_schema_done - s3_schema_start)
        ) * 1000

        proc_start = time.monotonic()
        schema_dict: dict[str, Any] = json.loads(schema_bytes)
        parsed_schema = fastavro.parse_schema(json.loads(schema_bytes))
        record: dict[str, Any] = fastavro.schemaless_reader(  # type: ignore[assignment]
            io.BytesIO(avro_payload), parsed_schema, parsed_schema
        )
        self.processing_duration_ms = (time.monotonic() - proc_start) * 1000

        self._logger.debug(
            "Deserialised alert", alert_id=alert_id, schema_id=schema_id
        )
        return _ParsedAlert(
            schema_dict=schema_dict,
            parsed_schema=parsed_schema,
            record=record,
            fetch_duration_ms=self.fetch_duration_ms,
            processing_duration_ms=self.processing_duration_ms,
        )
