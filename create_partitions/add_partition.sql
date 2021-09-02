CREATE OR REPLACE FUNCTION public.create_partitions(p_table text, p_interval interval, p_index_list text[] default '{}'::text[])
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE v_date timestamp without time zone;
BEGIN
--actions
 FOR v_date IN SELECT added_at::timestamp without time zone FROM generate_series(current_timestamp::date, (current_timestamp + p_interval)::date , interval '1 month') as gs(added_at) 
 LOOP
 EXECUTE 'CREATE TABLE IF NOT EXISTS '|| p_table || '_' || to_char(v_date, 'YYYYMM') ||' PARTITION OF ' || p_table || ' FOR VALUES FROM (''' || v_date::text || ''') TO (''' || (v_date +'1 month'::interval)::text ||''');';
 if NOT exists (select constraint_name from information_schema.table_constraints where table_name = 'actions_'|| to_char(v_date, 'YYYYMM') and constraint_type = 'PRIMARY KEY') then
  EXECUTE 'ALTER TABLE ' || p_table || '_' || to_char(v_date, 'YYYYMM') ||' ADD PRIMARY KEY ("id");';
  EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS index_actions_'|| to_char(v_date, 'YYYYMM') ||'_on_account_id_provider_id ON actions_' || to_char(v_date, 'YYYYMM') ||' (provider_account_id, provider_identifier) WHERE provider_account_id > 0;';
  EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS index_actions_' || to_char(v_date, 'YYYYMM') || '_on_provideridentifier ON actions_'|| to_char(v_date, 'YYYYMM') || ' USING BTREE (provider, provider_identifier);';
 END IF;
 END LOOP;
 
RETURN true;
END;
$function$
