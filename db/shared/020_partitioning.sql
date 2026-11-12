-- Helper for tables partitioned by order_day (one partition per simulated day).
-- create_day_partitions('orders', 0, 90) creates orders_d000 .. orders_d090.
-- Safe to call again: existing partitions are skipped.

CREATE FUNCTION create_day_partitions(p_parent text, p_from integer, p_to integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_day integer;
    v_created integer := 0;
    v_name text;
BEGIN
    FOR v_day IN p_from..p_to LOOP
        v_name := format('%s_d%s', p_parent, lpad(v_day::text, 3, '0'));
        IF to_regclass(v_name) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                v_name, p_parent, v_day, v_day + 1
            );
            v_created := v_created + 1;
        END IF;
    END LOOP;
    RETURN v_created;
END;
$$;
