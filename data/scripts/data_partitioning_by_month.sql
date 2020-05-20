-- Create tables by month
DO
$do$
DECLARE
  table_name text;
  month text;
  target_month  TEXT ARRAY  DEFAULT  ARRAY['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'];
BEGIN
  FOREACH month IN ARRAY target_month LOOP
    table_name = 'request_sets_' || month;
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', table_name);
    EXECUTE format('CREATE TABLE %I (CHECK ( extract(month from created_at) = ' || quote_literal(month) ||' )) INHERITS (public.request_sets)', table_name) USING month;
  END LOOP;
END;
$do$;
--

-- Function that routes by month
CREATE OR REPLACE FUNCTION request_sets_insert_by_month_trigger()
RETURNS TRIGGER AS $$
DECLARE
  target_month text;
  table_name text;
BEGIN
    SELECT cast(extract(month from NEW.created_at) AS TEXT) INTO target_month;
    table_name = 'public.request_sets_' || target_month;
    EXECUTE 'INSERT INTO ' || table_name || ' VALUES ($1.*)' USING NEW;

    RETURN NULL;
exception when others then
  raise notice '% %', SQLERRM, SQLSTATE;
  raise notice 'Insert failed ';
  INSERT INTO public.request_sets_misc VALUES (NEW.*);
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

-- Trigger for insert
DROP TRIGGER IF EXISTS insert_request_sets_trigger
  ON request_sets;
CREATE TRIGGER insert_request_sets_trigger
BEFORE INSERT ON request_sets
FOR EACH ROW EXECUTE PROCEDURE request_sets_insert_by_month_trigger();