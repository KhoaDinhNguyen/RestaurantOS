"""Fixed PostgreSQL advisory lock keys for commit-ordered admin feeds.

All writers of these tables must use the corresponding transaction lock.
Keys are application-wide constants, independent of region and connection.
"""

CLUSTER_EVENTS_LOCK_KEY = 51201
ANALYTICS_EXCEPTIONS_LOCK_KEY = 51202
