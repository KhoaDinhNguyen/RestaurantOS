-- apply_menu_version step 7, in the same transaction as the slice.
UPDATE node_state
SET menu_version = %(version)s;
