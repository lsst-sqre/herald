"""Tests for the AlertStore S3 key construction and access patterns."""

import gzip
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError

from herald.config import config
from herald.exceptions import AlertNotFoundError
from herald.storage import AlertStore


def _no_such_key() -> ClientError:
    return ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
    )


def _mock_s3(body_bytes: bytes) -> MagicMock:
    """Return an S3 client mock whose get_object returns the given bytes."""
    body = MagicMock()
    body.read = AsyncMock(return_value=body_bytes)
    client = MagicMock()
    client.get_object = AsyncMock(return_value={"Body": body})
    return client


@pytest.mark.asyncio
async def test_alert_key_six_char_prefix() -> None:
    s3 = _mock_s3(gzip.compress(b"\x00" * 5 + b"test"))
    store = AlertStore(s3_client=s3, config=config)

    await store.get_alert_bytes(1234567890)

    s3.get_object.assert_called_once_with(
        Bucket=config.s3_alerts_bucket,
        Key="v2/alerts/123456/1234567890.avro.gz",
    )


@pytest.mark.asyncio
async def test_alert_key_short_id_prefix() -> None:
    s3 = _mock_s3(gzip.compress(b"\x00" * 5 + b"test"))
    store = AlertStore(s3_client=s3, config=config)

    await store.get_alert_bytes(1)

    s3.get_object.assert_called_once_with(
        Bucket=config.s3_alerts_bucket,
        Key="v2/alerts/1/1.avro.gz",
    )


@pytest.mark.asyncio
async def test_alert_key_fallback_to_uncompressed() -> None:
    raw = b"\x00" * 5 + b"test"
    body = MagicMock()
    body.read = AsyncMock(return_value=raw)
    s3 = MagicMock()
    s3.get_object = AsyncMock(side_effect=[_no_such_key(), {"Body": body}])
    store = AlertStore(s3_client=s3, config=config)

    result = await store.get_alert_bytes(1234567890)

    assert s3.get_object.call_count == 2
    s3.get_object.assert_any_call(
        Bucket=config.s3_alerts_bucket,
        Key="v2/alerts/123456/1234567890.avro",
    )
    assert result == raw


@pytest.mark.asyncio
async def test_alert_not_found_when_both_keys_missing() -> None:
    s3 = MagicMock()
    s3.get_object = AsyncMock(side_effect=_no_such_key())
    store = AlertStore(s3_client=s3, config=config)

    with pytest.raises(AlertNotFoundError):
        await store.get_alert_bytes(1234567890)


@pytest.mark.asyncio
async def test_schema_cached_after_first_fetch() -> None:
    schema_bytes = b'{"type": "record", "name": "Alert", "fields": []}'
    body = MagicMock()
    body.read = AsyncMock(return_value=schema_bytes)

    s3 = MagicMock()
    s3.get_object = AsyncMock(return_value={"Body": body})
    store = AlertStore(s3_client=s3, config=config)
    result1 = await store.get_schema_bytes(1)
    result2 = await store.get_schema_bytes(1)

    assert s3.get_object.call_count == 1
    assert result1 == result2 == schema_bytes
