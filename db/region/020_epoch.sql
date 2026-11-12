-- Epoch fencing. Every write function calls assert_epoch first, inside its
-- transaction. A caller holding an old epoch gets SQLSTATE 'ROS01', which
-- Python turns into restaurantos.common.errors.StaleEpochError.

CREATE FUNCTION assert_epoch(p_epoch integer)
RETURNS void
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_epoch integer;
BEGIN
    SELECT epoch INTO STRICT v_epoch FROM node_state;
    IF v_epoch <> p_epoch THEN
        RAISE EXCEPTION 'stale epoch: node is at %, caller sent %', v_epoch, p_epoch
            USING ERRCODE = 'ROS01';
    END IF;
END;
$$;
