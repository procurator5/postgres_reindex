CREATE OR REPLACE FUNCTION public.create_partitions(p_interval interval)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE v_date timestamp without time zone;
BEGIN
 FOR v_date IN SELECT added_at::timestamp without time zone FROM generate_series(current_timestamp::date, (current_timestamp + p_interval)::date , interval '1 month') as gs(added_at) 
 LOOP
 --balance_transactions
 EXECUTE 'CREATE TABLE IF NOT EXISTS balance_transactions_' || to_char(v_date, 'YYYYMM') ||' PARTITION OF balance_transactions FOR VALUES FROM (''' || v_date::text || ''') TO (''' || (v_date +'1 month'::interval)::text ||''');';
 if NOT exists (select constraint_name from information_schema.table_constraints where table_name = 'balance_transactions_'|| to_char(v_date, 'YYYYMM') and constraint_type = 'PRIMARY KEY') then
  EXECUTE 'ALTER TABLE balance_transactions_' || to_char(v_date, 'YYYYMM') ||' ADD PRIMARY KEY ("id");';
  EXECUTE 'CREATE INDEX IF NOT EXISTS balance_transactions_'|| to_char(v_date, 'YYYYMM') ||'_account_id_created_at_idx ON balance_transactions_' || to_char(v_date, 'YYYYMM') ||' (account_id, created_at)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS balance_transactions_' || to_char(v_date, 'YYYYMM') || '_created_at_idx ON balance_transactions_'|| to_char(v_date, 'YYYYMM') || ' (created_at);';
  EXECUTE 'CREATE INDEX IF NOT EXISTS balance_transactions_' || to_char(v_date, 'YYYYMM') || '_reference_id_reference_type_idx ON balance_transactions_'|| to_char(v_date, 'YYYYMM') || ' (reference_id, reference_type);';
 END IF;
 --comp_point_transactions
 EXECUTE 'CREATE TABLE IF NOT EXISTS comp_point_transactions_' || to_char(v_date, 'YYYYMM') ||' PARTITION OF comp_point_transactions FOR VALUES FROM (''' || v_date::text || ''') TO (''' || (v_date +'1 month'::interval)::text ||''');';
 if NOT exists (select constraint_name from information_schema.table_constraints where table_name = 'comp_point_transactions_'|| to_char(v_date, 'YYYYMM') and constraint_type = 'PRIMARY KEY') then
  EXECUTE 'ALTER TABLE comp_point_transactions_' || to_char(v_date, 'YYYYMM') ||' ADD PRIMARY KEY ("id");';
  EXECUTE 'CREATE INDEX IF NOT EXISTS comp_point_transactions_'|| to_char(v_date, 'YYYYMM') ||'_comp_point_account_id_idx ON comp_point_transactions_' || to_char(v_date, 'YYYYMM') ||' (comp_point_account_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS comp_point_transactions_' || to_char(v_date, 'YYYYMM') || '_created_at_idx ON comp_point_transactions_'|| to_char(v_date, 'YYYYMM') || ' (created_at);';
 END IF;
 
 --events
 EXECUTE 'CREATE TABLE IF NOT EXISTS events_' || to_char(v_date, 'YYYYMM') ||' PARTITION OF events FOR VALUES FROM (''' || v_date::text || ''') TO (''' || (v_date +'1 month'::interval)::text ||''');';
 if NOT exists (select constraint_name from information_schema.table_constraints where table_name = 'events_'|| to_char(v_date, 'YYYYMM') and constraint_type = 'PRIMARY KEY') then
  EXECUTE 'ALTER TABLE events_' || to_char(v_date, 'YYYYMM') ||' ADD PRIMARY KEY ("id");';
  EXECUTE 'CREATE INDEX IF NOT EXISTS events_'|| to_char(v_date, 'YYYYMM') ||'_event_type_idx ON events_' || to_char(v_date, 'YYYYMM') ||' (event_type)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS events_' || to_char(v_date, 'YYYYMM') || '_subject_type_subject_id_created_at_idx ON events_'|| to_char(v_date, 'YYYYMM') || ' (subject_type, subject_id, created_at);';
 END IF;
 --games
 EXECUTE 'CREATE TABLE IF NOT EXISTS games_' || to_char(v_date, 'YYYYMM') ||' PARTITION OF games FOR VALUES FROM (''' || v_date::text || ''') TO (''' || (v_date +'1 month'::interval)::text ||''');';
 if NOT exists (select constraint_name from information_schema.table_constraints where table_name = 'games_'|| to_char(v_date, 'YYYYMM') and constraint_type = 'PRIMARY KEY') then 
  EXECUTE 'ALTER TABLE games_' || to_char(v_date, 'YYYYMM') ||' ADD PRIMARY KEY ("id");';
  EXECUTE 'CREATE INDEX IF NOT EXISTS index_games_' || to_char(v_date, 'YYYYMM') || '_account_id_created_at_idx ON games_'|| to_char(v_date, 'YYYYMM') ||' (account_id, created_at DESC) WHERE account_id IS NOT NULL;';
  EXECUTE 'CREATE INDEX IF NOT EXISTS index_games_' || to_char(v_date, 'YYYYMM') || '_bonus_issue_id_idx ON games_'|| to_char(v_date, 'YYYYMM') ||' (bonus_issue_id) WHERE bonus_issue_id IS NOT NULL AND finished_at IS NULL';
  EXECUTE 'CREATE INDEX IF NOT EXISTS index_games_' || to_char(v_date, 'YYYYMM') || '_external_id_idx ON games_'|| to_char(v_date, 'YYYYMM') ||' (external_id) WHERE external_id IS NOT NULL';
  EXECUTE 'CREATE INDEX IF NOT EXISTS index_games_' || to_char(v_date, 'YYYYMM') || '_game_table_id_idx ON games_'|| to_char(v_date, 'YYYYMM') ||' (game_table_id)';
 END IF; 
 --versions
 EXECUTE 'CREATE TABLE IF NOT EXISTS versions_' || to_char(v_date, 'YYYYMM') ||' PARTITION OF versions FOR VALUES FROM (''' || v_date::text || ''') TO (''' || (v_date +'1 month'::interval)::text ||''');';
 if NOT exists (select constraint_name from information_schema.table_constraints where table_name = 'versions_'|| to_char(v_date, 'YYYYMM') and constraint_type = 'PRIMARY KEY') then
  EXECUTE 'ALTER TABLE versions_' || to_char(v_date, 'YYYYMM') ||' ADD PRIMARY KEY ("id");';
  EXECUTE 'CREATE INDEX IF NOT EXISTS versions_'|| to_char(v_date, 'YYYYMM') ||'_item_type_item_id_idx ON versions_' || to_char(v_date, 'YYYYMM') ||' (item_type, item_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS versions_' || to_char(v_date, 'YYYYMM') || '_whodunnit_type_whodunnit_id_idx ON versions_'|| to_char(v_date, 'YYYYMM') || ' (whodunnit_type, whodunnit_id);';
 END IF;
 END LOOP;


RETURN true;
END;
$function$
