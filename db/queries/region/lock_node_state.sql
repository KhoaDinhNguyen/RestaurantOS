-- apply_menu_version step 2. The version this node has, locked: two applies
-- run one after the other, and the second sees the first's version. That can
-- happen, because a call that missed its deadline is not cancelled (run_on)
-- and may still be running when the publisher retries.
SELECT menu_version
FROM node_state
FOR UPDATE;
