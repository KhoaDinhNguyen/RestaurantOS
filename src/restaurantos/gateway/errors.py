"""Gateway-specific errors."""


class HQUnreachableError(Exception):
    """The HQ database could not be reached in time."""
