"""Tests for the herald.handlers.external module and routes."""

import io
import json
import struct
from typing import Any
from unittest.mock import MagicMock

import fastavro
import pytest
from httpx import AsyncClient

from herald.config import config
from herald.exceptions import AlertNotFoundError, SchemaNotFoundError


def _make_alert_bytes(alert: dict, schema: dict) -> bytes:
    """Serialise an alert dict to Confluent wire format.

    Returns the decompressed bytes as ``AlertStore.get_alert_bytes`` would
    return them.

    Parameters
    ----------
    alert
        Avro schema dict matching the alert structure.
    schema
        Avro schema dict matching the alert structure.

    Returns
    -------
    bytes
        The alert serialised in Confluent wire format (no compression).
    """
    parsed = fastavro.parse_schema(schema)

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, alert)
    avro_payload = buf.getvalue()

    schema_id = 1
    header = struct.pack(">BI", 0x00, schema_id)
    return header + avro_payload


_SIMPLE_SCHEMA = {
    "type": "record",
    "name": "Alert",
    "namespace": "lsst.v7",
    "fields": [
        {"name": "alertId", "type": "long"},
        {"name": "diaSourceId", "type": "long"},
        {"name": "ra", "type": "double"},
        {"name": "dec", "type": "double"},
    ],
}

_SAMPLE_ALERT = {
    "alertId": 12345,
    "diaSourceId": 12345,
    "ra": 123.456,
    "dec": -45.678,
}


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /api/herald/``."""
    response = await client.get("/api/herald/")
    assert response.status_code == 200
    data = response.json()
    metadata = data["metadata"]
    assert metadata["name"] == config.name
    assert isinstance(metadata["version"], str)
    assert isinstance(metadata["description"], str)
    assert isinstance(metadata["repository_url"], str)
    assert isinstance(metadata["documentation_url"], str)


@pytest.mark.asyncio
async def test_get_alert(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    """Test ``GET /api/herald/alerts/{alert_id}`` with a valid alert."""
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get("/api/herald/alerts/12345")

    assert response.status_code == 200
    data = response.json()
    assert data["alertId"] == 12345
    assert data["diaSourceId"] == 12345
    assert data["ra"] == pytest.approx(123.456)
    assert data["dec"] == pytest.approx(-45.678)


@pytest.mark.asyncio
async def test_get_alert_not_found(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    mock_store.get_alert_bytes.side_effect = AlertNotFoundError(99999)

    response = await client_with_mock_store.get("/api/herald/alerts/99999")

    assert response.status_code == 404
    assert "99999" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_alert_schema(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/herald/alerts/12345/schema"
    )

    assert response.status_code == 200
    schema = response.json()
    assert schema["name"] == "Alert"
    assert schema["namespace"] == "lsst.v7"
    assert len(schema["fields"]) == 4


@pytest.mark.asyncio
async def test_get_alert_schema_not_found(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.side_effect = SchemaNotFoundError(1)

    response = await client_with_mock_store.get(
        "/api/herald/alerts/12345/schema"
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_alert_avro_format(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/herald/alerts/12345",
        headers={"Accept": "application/avro"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/avro"

    records = list(fastavro.reader(io.BytesIO(response.content)))
    assert len(records) == 1
    record: dict[str, Any] = records[0]  # type: ignore[assignment]
    assert record["alertId"] == 12345
    assert record["diaSourceId"] == 12345


@pytest.mark.asyncio
async def test_get_alert_malformed_bytes(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    mock_store.get_alert_bytes.return_value = b"\x1f\x8b" + b"\x00" * 10
    response = await client_with_mock_store.get("/api/herald/alerts/12345")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_alert_short_bytes(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    mock_store.get_alert_bytes.return_value = b"\x00" * 3
    response = await client_with_mock_store.get("/api/herald/alerts/12345")
    assert response.status_code == 404
