-- restock step 2, before the vendor's counters: the lock order
-- apply_menu_version uses. Apply holds this row FOR UPDATE, so a restock waits
-- for an apply in flight to commit and cannot clear the stock mark of a product
-- that apply just added with empty counters. FOR SHARE, not FOR UPDATE, so
-- restocks of different vendors do not wait on each other.
SELECT 1
FROM node_state
FOR SHARE;
