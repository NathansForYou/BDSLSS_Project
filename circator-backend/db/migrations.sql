--
-- Synchronization state migration (build 81)

create or replace function migrate_last_acquired_v81() returns void as
$$
begin
  update users
    set last_acquired = R.new_la
    from (
      select id as new_id,
             jsonb_object_agg(key,
                value || format('{"ios_log": {"0": {"seq_id": %s, "seq_data": "%s"}}}',
                                value#>>'{id}', value#>>'{data}')::jsonb
              ) as new_la
      from users, lateral jsonb_each(last_acquired)
      where value ? 'id' and value ? 'data'
      group by id
    ) R
    where id = R.new_id;
  return;
end
$$
language plpgsql;