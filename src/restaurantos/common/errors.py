"""Errors that cross component boundaries."""

from __future__ import annotations

STALE_EPOCH_SQLSTATE = "ROS01"
"""SQLSTATE raised by the SQL function assert_epoch()."""


class RestaurantOSError(Exception):
    """Base class for our own errors."""


class StaleEpochError(RestaurantOSError):
    """The caller's epoch is not the node's epoch.

    The caller should refresh the shard directory and retry once.
    """


class UnknownVendorError(RestaurantOSError):
    """The vendor does not exist, or does not belong to this region."""


class RegionUnreachableError(RestaurantOSError):
    """The region's primary could not be reached in time."""
