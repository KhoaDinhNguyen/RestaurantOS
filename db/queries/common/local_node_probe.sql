-- What a region-local process needs to know about one of its nodes:
-- is it a standby, and which epoch does it carry? Works on a hot standby.
SELECT
    epoch,
    pg_is_in_recovery() AS in_recovery
FROM node_state;
