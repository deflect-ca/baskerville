-- Create tables by target first letter - misc for those who have null or weird values
DO
$do$
DECLARE
  table_name text;
  letter text;
  target_letter  TEXT ARRAY  DEFAULT  ARRAY['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
                                                  'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'misc'];
BEGIN
  FOREACH letter IN ARRAY target_letter LOOP
    table_name = 'request_sets_' || letter;
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', table_name);
    EXECUTE format('CREATE TABLE %I (CHECK ( LEFT(target, 1) = ' || quote_literal(letter) ||' )) INHERITS (public.request_sets)', table_name) USING letter;
  END LOOP;
END;
$do$;

-- Function that routes by target's first letter
CREATE OR REPLACE FUNCTION request_sets_insert_by_host_trigger()
RETURNS TRIGGER AS $$
DECLARE
  target_letter text;
  table_name text;
BEGIN
    SELECT LEFT(NEW.target, 1) INTO target_letter;
    table_name = 'public.request_sets_' || target_letter;
    EXECUTE 'INSERT INTO ' || table_name || ' VALUES ($1.*)' USING NEW;

    RETURN NULL;
exception when others then
  raise notice '% %', SQLERRM, SQLSTATE;
  raise notice 'Insert failed ';
  INSERT INTO public.request_sets_everything_else VALUES (NEW.*);
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

-- Trigger for insert
DROP TRIGGER IF EXISTS insert_request_sets_trigger
  ON request_sets;
CREATE TRIGGER insert_request_sets_trigger
BEFORE INSERT ON request_sets
FOR EACH ROW EXECUTE PROCEDURE request_sets_insert_by_host_trigger();