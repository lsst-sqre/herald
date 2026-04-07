"""Package-wide constants for the herald service."""

__all__ = ["IAU_ALERT_PREFIX"]

IAU_ALERT_PREFIX = "LSST-AP-DS-"
"""IAU alert identifier prefix for LSST DIA sources.

Alert IDs may be expressed as integers or in the full IAU form
``LSST-AP-DS-{diaSourceId}``.
"""
