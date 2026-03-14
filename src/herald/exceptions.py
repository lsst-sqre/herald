"""Custom exceptions for the herald service."""

__all__ = ["AlertNotFoundError", "SchemaNotFoundError"]


class AlertNotFoundError(Exception):
    """Raised when an alert cannot be found in the object store."""

    def __init__(self, alert_id: int) -> None:
        self.alert_id = alert_id
        super().__init__(f"Alert {alert_id} not found")


class SchemaNotFoundError(Exception):
    """Raised when an Avro schema cannot be found in the object store."""

    def __init__(self, schema_id: int) -> None:
        self.schema_id = schema_id
        super().__init__(f"Schema {schema_id} not found")
