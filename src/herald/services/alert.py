"""Alert retrieval and deserialisation service."""

from __future__ import annotations

import io
import json
import struct
from typing import Any

import fastavro
from structlog.stdlib import BoundLogger

from ..exceptions import AlertNotFoundError
from ..storage import AlertStore

__all__ = ["AlertService"]

# Confluent wire format magic byte
_CONFLUENT_MAGIC = 0x00
_HEADER_SIZE = 5  # 1 magic byte + 4 byte schema ID


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

    async def get_alert(self, alert_id: int) -> dict[str, Any]:
        """Retrieve an alert packet and return it as a Python dict.

        Fetches the gzip-compressed Avro binary from S3, strips the Confluent
        wire format header, fetches the matching schema, and deserialises the
        record with fastavro.

        Parameters
        ----------
        alert_id
            Alert ID (currently equal to diaSourceId).

        Returns
        -------
        dict
            Deserialised alert record.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        ValueError
            If the stored bytes do not start with the expected Confluent magic
            byte.
        """
        _, record = await self._fetch_record(alert_id)
        return record

    async def get_alert_avro(self, alert_id: int) -> bytes:
        """Retrieve an alert packet as an Avro OCF container file.

        Returns an Avro Object Container File (OCF) with the schema embedded,
        so the response is self-describing and can be read by any Avro library
        without a separate schema request.

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
        ValueError
            If the stored bytes do not start with the expected Confluent magic
            byte.
        """
        parsed_schema, record = await self._fetch_record(alert_id)
        buf = io.BytesIO()
        fastavro.writer(buf, parsed_schema, [record])
        return buf.getvalue()

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
        # We need to fetch the alert to read the Confluent header and get the
        # schema ID. This is a bit inefficient but avoids the need for a
        # separate metadata store mapping alert IDs to schema IDs.
        # Since schema requests will be rare and alert packets are small,
        # this is an acceptable
        raw = await self._store.get_alert_bytes(alert_id)
        schema_id, _ = self._parse_confluent_header(alert_id, raw)
        schema_bytes = await self._store.get_schema_bytes(schema_id)
        schema: dict[str, Any] = json.loads(schema_bytes)
        return schema

    async def _fetch_record(self, alert_id: int) -> tuple[Any, dict[str, Any]]:
        """Fetch an alert from S3 and deserialise it.

        Shared implementation used by `get_alert` and `get_alert_avro`.

        Parameters
        ----------
        alert_id
            Alert ID (currently equal to diaSourceId).

        Returns
        -------
        tuple[Any, dict[str, Any]]
            ``(parsed_schema, record)`` where ``parsed_schema`` is the
            fastavro-compiled schema and ``record`` is the deserialised alert.

        Raises
        ------
        AlertNotFoundError
            If the alert does not exist in the archive.
        ValueError
            If the stored bytes do not start with the expected Confluent magic
            byte.
        """
        raw = await self._store.get_alert_bytes(alert_id)
        schema_id, avro_payload = self._parse_confluent_header(alert_id, raw)
        schema_bytes = await self._store.get_schema_bytes(schema_id)
        parsed_schema = fastavro.parse_schema(json.loads(schema_bytes))
        record: dict[str, Any] = fastavro.schemaless_reader(  # type: ignore[assignment]
            io.BytesIO(avro_payload), parsed_schema, parsed_schema
        )
        self._logger.debug(
            "Deserialised alert", alert_id=alert_id, schema_id=schema_id
        )
        return parsed_schema, record

    def _parse_confluent_header(
        self, alert_id: int, raw: bytes
    ) -> tuple[int, bytes]:
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
        AlertNotFoundError
            Re-raised if the bytes are empty (defensive).
        ValueError
            If the magic byte is unexpected.
        """
        if len(raw) < _HEADER_SIZE:
            raise AlertNotFoundError(alert_id)

        magic = raw[0]
        if magic != _CONFLUENT_MAGIC:
            raise ValueError(
                f"Alert {alert_id}: unexpected magic byte {magic:#04x},"
                f" expected {_CONFLUENT_MAGIC:#04x}"
            )

        (schema_id,) = struct.unpack(">I", raw[1:_HEADER_SIZE])
        avro_payload = raw[_HEADER_SIZE:]
        return schema_id, avro_payload
