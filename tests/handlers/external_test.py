"""Tests for the herald.handlers.external module and routes."""

import base64
import io
import json
import struct
import xml.etree.ElementTree as ET
from typing import Any
from unittest.mock import MagicMock

import fastavro
import numpy as np
import pytest
from astropy.io import fits
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
        Alert data dict matching the schema structure.
    schema
        Avro schema dict describing the alert structure.

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

# Minimal schema that satisfies get_alert_fits field navigation.
_FITS_SCHEMA: dict = {
    "type": "record",
    "name": "alert",
    "namespace": "lsst.v10_0",
    "fields": [
        {"name": "diaSourceId", "type": "long"},
        {"name": "observation_reason", "type": ["null", "string"]},
        {"name": "target_name", "type": ["null", "string"]},
        {
            "name": "diaSource",
            "type": {
                "type": "record",
                "name": "diaSource",
                "fields": [
                    {"name": "diaSourceId", "type": "long"},
                    {"name": "ra", "type": "double"},
                    {"name": "dec", "type": "double"},
                ],
            },
        },
        {
            "name": "prvDiaSources",
            "type": ["null", {"type": "array", "items": "diaSource"}],
        },
        {
            "name": "prvDiaForcedSources",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "diaForcedSource",
                        "fields": [
                            {"name": "diaForcedSourceId", "type": "long"},
                            {"name": "ra", "type": "double"},
                        ],
                    },
                },
            ],
        },
        {
            "name": "diaObject",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "diaObject",
                    "fields": [{"name": "diaObjectId", "type": "long"}],
                },
            ],
        },
        {
            "name": "ssSource",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "ssSource",
                    "fields": [{"name": "diaSourceId", "type": "long"}],
                },
            ],
        },
        {
            "name": "mpc_orbits",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "mpc_orbits",
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "designation", "type": "string"},
                    ],
                },
            ],
        },
        {"name": "cutoutDifference", "type": ["null", "bytes"]},
        {"name": "cutoutScience", "type": ["null", "bytes"]},
        {"name": "cutoutTemplate", "type": ["null", "bytes"]},
    ],
}

_FITS_ALERT: dict = {
    "diaSourceId": 12345,
    "observation_reason": None,
    "target_name": None,
    "diaSource": {"diaSourceId": 12345, "ra": 123.456, "dec": -45.678},
    "prvDiaSources": None,
    "prvDiaForcedSources": None,
    "diaObject": None,
    "ssSource": None,
    "mpc_orbits": None,
    "cutoutDifference": None,
    "cutoutScience": None,
    "cutoutTemplate": None,
}


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /api/alerts/``."""
    response = await client.get("/api/alerts/")
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
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get("/api/alerts?ID=12345")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-avro-ocf"
    assert "attachment" in response.headers.get("content-disposition", "")

    records = list(fastavro.reader(io.BytesIO(response.content)))
    assert len(records) == 1
    record: dict[str, Any] = records[0]  # type: ignore[assignment]
    assert record["alertId"] == 12345
    assert record["diaSourceId"] == 12345


@pytest.mark.asyncio
async def test_get_alert_iau_id(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/alerts?ID=LSST-AP-DS-12345"
    )

    assert response.status_code == 200
    mock_store.get_alert_bytes.assert_called_once_with(12345)


@pytest.mark.asyncio
async def test_get_alert_invalid_id(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    response = await client_with_mock_store.get("/api/alerts?ID=not-a-number")
    assert response.status_code == 400
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert "not-a-number" in response.text


@pytest.mark.asyncio
async def test_get_alert_non_lsst_id(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    response = await client_with_mock_store.get(
        "/api/alerts?ID=LSST-OTHER-12345"
    )
    assert response.status_code == 400
    assert "Non-LSST" in response.text


@pytest.mark.asyncio
async def test_get_alert_fits_format(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_FITS_ALERT, _FITS_SCHEMA)
    schema_bytes = json.dumps(_FITS_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=fits"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/fits"
    assert "attachment" in response.headers.get("content-disposition", "")

    with fits.open(io.BytesIO(response.content)) as hdul:
        extnames = [hdu.name for hdu in hdul]
        assert "ALERT" in extnames
        assert "DIASOURCE" in extnames
        assert "FORCEDPHOT" in extnames
        assert "SSSOURCE" in extnames
        assert "DIAOBJECT" not in extnames
        assert "MPCORBIT" not in extnames


@pytest.mark.asyncio
async def test_get_alert_fits_application_mimetype(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_FITS_ALERT, _FITS_SCHEMA)
    schema_bytes = json.dumps(_FITS_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=application/fits"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/fits"


@pytest.mark.asyncio
async def test_get_alert_invalid_responseformat(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=text/html"
    )
    assert response.status_code == 415
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert "text/html" in response.text


@pytest.mark.asyncio
async def test_get_alert_json_format(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=json"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    data = response.json()
    assert data["alertId"] == 12345
    assert data["ra"] == pytest.approx(123.456)


@pytest.mark.asyncio
async def test_get_alert_json_application_mimetype(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=application/json"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_get_alert_json_encodes_bytes(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    stamp = _make_fits_stamp()
    alert = {**_FITS_ALERT, "cutoutDifference": stamp}
    alert_bytes = _make_alert_bytes(alert, _FITS_SCHEMA)
    schema_bytes = json.dumps(_FITS_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=json"
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data["cutoutDifference"], str)
    assert base64.b64decode(data["cutoutDifference"]) == stamp
    assert data["cutoutScience"] is None
    assert data["cutoutTemplate"] is None


@pytest.mark.asyncio
async def test_get_alert_not_found(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    mock_store.get_alert_bytes.side_effect = AlertNotFoundError(99999)

    response = await client_with_mock_store.get("/api/alerts?ID=99999")

    assert response.status_code == 404
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert "No data found for alert ID" in response.text
    assert "99999" in response.text


@pytest.mark.asyncio
async def test_get_alert_schema(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_SAMPLE_ALERT, _SIMPLE_SCHEMA)
    schema_bytes = json.dumps(_SIMPLE_SCHEMA).encode()

    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = schema_bytes

    response = await client_with_mock_store.get("/api/alerts/schema?ID=12345")

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
    response = await client_with_mock_store.get("/api/alerts/schema?ID=12345")
    assert response.status_code == 404


def _make_fits_stamp() -> bytes:
    """Create a minimal valid FITS image stamp in memory."""
    hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    buf = io.BytesIO()
    hdu.writeto(buf)
    return buf.getvalue()


@pytest.mark.asyncio
async def test_get_alert_cutouts(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    stamp = _make_fits_stamp()
    alert = {
        "alertId": 12345,
        "diaSourceId": 12345,
        "ra": 123.456,
        "dec": -45.678,
        "cutoutDifference": stamp,
        "cutoutScience": stamp,
        "cutoutTemplate": stamp,
    }
    schema = {
        "type": "record",
        "name": "Alert",
        "namespace": "lsst.v7",
        "fields": [
            {"name": "alertId", "type": "long"},
            {"name": "diaSourceId", "type": "long"},
            {"name": "ra", "type": "double"},
            {"name": "dec", "type": "double"},
            {"name": "cutoutDifference", "type": ["null", "bytes"]},
            {"name": "cutoutScience", "type": ["null", "bytes"]},
            {"name": "cutoutTemplate", "type": ["null", "bytes"]},
        ],
    }
    alert_bytes = _make_alert_bytes(alert, schema)
    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = json.dumps(schema).encode()

    response = await client_with_mock_store.get("/api/alerts/cutouts?ID=12345")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/fits"

    with fits.open(io.BytesIO(response.content)) as hdul:
        extnames = [hdu.name for hdu in hdul]
        assert "DIFFIM" in extnames
        assert "SCIENCE" in extnames
        assert "TEMPLATE" in extnames


@pytest.mark.asyncio
async def test_get_alert_cutouts_none_present(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    alert_bytes = _make_alert_bytes(_FITS_ALERT, _FITS_SCHEMA)
    mock_store.get_alert_bytes.return_value = alert_bytes
    mock_store.get_schema_bytes.return_value = json.dumps(
        _FITS_SCHEMA
    ).encode()

    response = await client_with_mock_store.get("/api/alerts/cutouts?ID=12345")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/fits"

    with fits.open(io.BytesIO(response.content)) as hdul:
        assert len(hdul) == 1
        assert hdul[0].name == "PRIMARY"


@pytest.mark.asyncio
async def test_get_alert_links(
    client_with_mock_store: AsyncClient,
) -> None:
    response = await client_with_mock_store.get("/api/alerts/links?ID=12345")

    assert response.status_code == 200
    assert "application/x-votable+xml" in response.headers["content-type"]

    ns = {"vot": "http://www.ivoa.net/xml/VOTable/v1.3"}
    root = ET.fromstring(response.content)  # noqa: S314
    rows = root.findall(".//vot:TR", ns)
    assert len(rows) == 4

    def _col(row: ET.Element, index: int) -> str:
        return row.findall("vot:TD", ns)[index].text or ""

    access_urls = [_col(row, 1) for row in rows]
    semantics = [_col(row, 5) for row in rows]

    assert any("12345" in url and "cutouts" in url for url in access_urls)
    assert any("12345" in url and "schema" in url for url in access_urls)
    assert "#this" in semantics
    assert "#cutout" in semantics
    assert "#detached-header" in semantics


@pytest.mark.asyncio
async def test_get_alert_links_invalid_id(
    client_with_mock_store: AsyncClient,
) -> None:
    response = await client_with_mock_store.get(
        "/api/alerts/links?ID=invalid-id"
    )
    assert response.status_code == 400
    assert "invalid-id" in response.text


@pytest.mark.asyncio
async def test_get_alert_unknown_param(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    response = await client_with_mock_store.get("/api/alerts?ID=12345&foo=bar")
    assert response.status_code == 400
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert "Invalid parameter 'foo'" in response.text
    assert "foo=bar" in response.text


@pytest.mark.asyncio
async def test_get_alert_cutouts_unknown_param(
    client_with_mock_store: AsyncClient,
) -> None:
    response = await client_with_mock_store.get(
        "/api/alerts/cutouts?ID=12345&foo=bar"
    )
    assert response.status_code == 400
    assert "Invalid parameter 'foo'" in response.text


@pytest.mark.asyncio
async def test_get_alert_schema_unknown_param(
    client_with_mock_store: AsyncClient,
) -> None:
    response = await client_with_mock_store.get(
        "/api/alerts/schema?ID=12345&foo=bar"
    )
    assert response.status_code == 400
    assert "Invalid parameter 'foo'" in response.text


@pytest.mark.asyncio
async def test_get_alert_links_unknown_param(
    client_with_mock_store: AsyncClient,
) -> None:
    response = await client_with_mock_store.get(
        "/api/alerts/links?ID=12345&foo=bar"
    )
    assert response.status_code == 400
    assert "Invalid parameter 'foo' " in response.text


@pytest.mark.asyncio
async def test_get_alert_json_nan_becomes_null(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    schema = {
        "type": "record",
        "name": "Alert",
        "namespace": "lsst.v7",
        "fields": [
            {"name": "alertId", "type": "long"},
            {"name": "psfLnL", "type": "float"},
        ],
    }
    alert = {"alertId": 12345, "psfLnL": float("nan")}
    mock_store.get_alert_bytes.return_value = _make_alert_bytes(alert, schema)
    mock_store.get_schema_bytes.return_value = json.dumps(schema).encode()

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=json"
    )

    assert response.status_code == 200
    assert response.json()["psfLnL"] is None


@pytest.mark.asyncio
async def test_get_alert_corrupt_magic_byte(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    corrupt_bytes = bytes([0x01, 0x00, 0x00, 0x00, 0x01]) + b"payload"
    mock_store.get_alert_bytes.return_value = corrupt_bytes
    response = await client_with_mock_store.get("/api/alerts?ID=12345")
    assert response.status_code == 500


@pytest.mark.asyncio
async def test_get_alert_cutouts_not_found(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    mock_store.get_alert_bytes.side_effect = AlertNotFoundError(12345)
    response = await client_with_mock_store.get("/api/alerts/cutouts?ID=12345")
    assert response.status_code == 404
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


@pytest.mark.asyncio
async def test_get_alert_fits_trigger_and_iau_id(
    client_with_mock_store: AsyncClient, mock_store: MagicMock
) -> None:
    prv_source = {"diaSourceId": 99999, "ra": 1.0, "dec": 2.0}
    alert = {**_FITS_ALERT, "prvDiaSources": [prv_source]}
    mock_store.get_alert_bytes.return_value = _make_alert_bytes(
        alert, _FITS_SCHEMA
    )
    mock_store.get_schema_bytes.return_value = json.dumps(
        _FITS_SCHEMA
    ).encode()

    response = await client_with_mock_store.get(
        "/api/alerts?ID=12345&RESPONSEFORMAT=fits"
    )
    assert response.status_code == 200

    with fits.open(io.BytesIO(response.content)) as hdul:
        data = hdul["DIASOURCE"].data
        assert data["trigger"][0]
        assert not data["trigger"][1]
        assert data["iau_id"][0].strip() == "LSST-AP-DS-12345"
        assert data["iau_id"][1].strip() == "LSST-AP-DS-99999"
