"""Configuration definition."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.logging import LogLevel, Profile
from safir.metrics import MetricsConfiguration, metrics_configuration_factory

__all__ = ["Config", "config"]


class Config(BaseSettings):
    """Configuration for herald."""

    model_config = SettingsConfigDict(
        env_prefix="HERALD_", case_sensitive=False
    )

    name: str = Field("herald", title="Name of application")

    path_prefix: str = Field("/api/alerts", title="URL prefix for application")

    log_level: LogLevel = Field(
        LogLevel.INFO, title="Log level of the application's logger"
    )

    log_profile: Profile = Field(
        Profile.development, title="Application logging profile"
    )

    s3_alerts_bucket: str = Field(
        ...,
        title="S3 bucket name for alert packets",
        description="Bucket containing the alert archive packets.",
    )

    s3_schemas_bucket: str = Field(
        ...,
        title="S3 bucket name for Avro schemas",
        description="Bucket containing the Avro schema JSON files.",
    )

    s3_alerts_prefix: str = Field(
        "v2/alerts",
        title="S3 key prefix for alert packets",
        description=("Path prefix for alert keys within the alerts bucket."),
    )

    s3_schemas_prefix: str = Field(
        "v2/schemas",
        title="S3 key prefix for Avro schemas",
        description=("Path prefix for schema keys within the schemas bucket."),
    )

    s3_endpoint_url: str | None = Field(
        None,
        title="S3 endpoint URL",
        description=(
            "Override endpoint URL for S3-compatible stores such as"
            " MinIO or Ceph."
        ),
    )

    s3_region: str | None = Field(
        None,
        title="S3 region",
        description=(
            "Region passed to the S3 client. Ignored by MinIO/Ceph."
            " Required when using GCS S3-compatible API or AWS S3."
        ),
    )

    aws_access_key_id: SecretStr | None = Field(
        None, title="AWS/S3 access key ID"
    )

    aws_secret_access_key: SecretStr | None = Field(
        None, title="AWS/S3 secret access key"
    )

    metrics: MetricsConfiguration = Field(
        default_factory=metrics_configuration_factory,
        title="Metrics configuration",
    )

    slack_webhook: SecretStr | None = Field(
        None,
        title="Slack webhook for alerts",
        description="If set, alerts will be posted to this Slack webhook",
    )

    @property
    def s3_credentials(self) -> dict[str, str | None]:
        """Unwrapped S3 credentials for passing to the aiobotocore client."""
        return {
            "aws_access_key_id": (
                self.aws_access_key_id.get_secret_value()
                if self.aws_access_key_id
                else None
            ),
            "aws_secret_access_key": (
                self.aws_secret_access_key.get_secret_value()
                if self.aws_secret_access_key
                else None
            ),
        }


config = Config()
"""Configuration for herald."""
