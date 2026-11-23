-- Coordinator-owned lock, taken on the HQ candidate, including during recovery.
SELECT pg_try_advisory_xact_lock(51203);
