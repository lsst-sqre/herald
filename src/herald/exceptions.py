"""Custom exceptions for the herald service."""

__all__ = [
    "AlertNotFoundError",
    "CorruptAlertError",
    "SchemaNotFoundError",
]


class CorruptAlertError(Exception):
    """Raised when stored alert bytes are structurally invalid. This
    indicates data corruption and should propagate as a 500.
    """

    def __init__(self, alert_id: int, detail: str) -> None:
        self.alert_id = alert_id
        super().__init__(f"Alert {alert_id}: {detail}")


class AlertNotFoundError(Exception):
    """Raised when an alert cannot be found in the object store."""

    def __init__(self, alert_id: int) -> None:
        self.alert_id = alert_id
        super().__init__(f"No data found for alert ID '{alert_id}'")


class SchemaNotFoundError(Exception):
    """Raised when an Avro schema cannot be found in the object store."""

    def __init__(self, schema_id: int) -> None:
        self.schema_id = schema_id
        super().__init__(f"Schema {schema_id} not found")
