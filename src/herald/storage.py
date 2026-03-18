"""S3 storage layer for the alert archive."""

from __future__ import annotations

import gzip
from typing import Any

import structlog
from botocore.exceptions import ClientError

from .config import Config
from .exceptions import AlertNotFoundError, SchemaNotFoundError

__all__ = ["AlertStore"]


class AlertStore:
    """Async interface to the S3-compatible alert archive.

    Parameters
    ----------
    s3_client
        An aiobotocore S3 client, created and owned by the caller (lifespan).
    config
        Application configuration.
    """

    def __init__(self, *, s3_client: Any, config: Config) -> None:
        self._client = s3_client
        self._config = config
        self._logger = structlog.get_logger(__name__)
        # Schemas are immutable once written; cache them for the process
        # lifetime to avoid repeated S3 fetches across requests.
        self._schema_cache: dict[int, bytes] = {}

    def _alert_key(self, alert_id: int, *, compressed: bool = True) -> str:
        """Return the S3 key for an alert object.

        In the current alerts archive design, alerts are sharded by the
        first 6 digits of the alert ID string. If the sharding strategy
        changes this is the only place that needs updating.

        Parameters
        ----------
        alert_id
            The numeric alert ID.
        compressed
            If ``True``, return the key for the gzip-compressed form
            otherwise return the uncompressed key

        Returns
        -------
        str
            The S3 object key.
        """
        alert_id_str = str(alert_id)
        shard = alert_id_str[:6]
        ext = ".avro.gz" if compressed else ".avro"
        return f"{self._config.s3_alerts_prefix}/{shard}/{alert_id_str}{ext}"

    async def get_alert_bytes(self, alert_id: int) -> bytes:
        """Fetch and decompress a raw alert from S3.

        Tries the gzip-compressed form first (``*.avro.gz``), then falls back
        to the uncompressed form (``*.avro``).

        Parameters
        ----------
        alert_id
            The alert ID (currently equal to diaSourceId).

        Returns
        -------
        bytes
            Decompressed alert bytes in Confluent wire format.

        Raises
        ------
        AlertNotFoundError
            If neither the compressed nor the uncompressed object exists.
        """
        gz_key = self._alert_key(alert_id, compressed=True)
        self._logger.debug(
            "Fetching alert from S3",
            bucket=self._config.s3_alerts_bucket,
            key=gz_key,
        )
        try:
            response = await self._client.get_object(
                Bucket=self._config.s3_alerts_bucket, Key=gz_key
            )
            compressed = await response["Body"].read()
            return gzip.decompress(compressed)
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise

        avro_key = self._alert_key(alert_id, compressed=False)
        self._logger.debug(
            "Compressed alert not found, trying uncompressed",
            bucket=self._config.s3_alerts_bucket,
            key=avro_key,
        )
        try:
            response = await self._client.get_object(
                Bucket=self._config.s3_alerts_bucket, Key=avro_key
            )
            return await response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise AlertNotFoundError(alert_id) from e
            raise

    async def get_schema_bytes(self, schema_id: int) -> bytes:
        """Fetch Avro schema JSON from S3, with in-memory caching.

        Parameters
        ----------
        schema_id
            The 4-byte integer schema ID extracted from the Confluent wire
            format header.

        Returns
        -------
        bytes
            Raw UTF-8 encoded Avro schema JSON.

        Raises
        ------
        SchemaNotFoundError
            If the schema object does not exist in the bucket.
        """
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        key = f"{self._config.s3_schemas_prefix}/{schema_id}.json"
        self._logger.debug(
            "Fetching schema from S3",
            bucket=self._config.s3_schemas_bucket,
            key=key,
        )

        try:
            response = await self._client.get_object(
                Bucket=self._config.s3_schemas_bucket, Key=key
            )
            schema_bytes = await response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise SchemaNotFoundError(schema_id) from e
            raise

        self._schema_cache[schema_id] = schema_bytes
        return schema_bytes
