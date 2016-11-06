-----------------------
-- Measures ETL.
-- This provides supporting relations to help launch our periodic
-- ETL process to load unstructured json measures into structured MC schema.
--
-- The ETL UDFs are generated from measures_etl.rb

-- The ETL launch table has triggers registered on it to perform ETL functionality in the background.
-- This table is directly inserted into by AWS Lambda jobs, ensuring that our Lambda tasks run for a
-- short duration, while the database can then asynchronously perform measure loading.
drop table if exists measures_etl_launch;
create table measures_etl_launch (
  job_id       bigserial primary key,
  dataset_type smallint not null,
  launch_ts    timestamptz default now()
);

-- A table for tracking completed ETL job statistics.
drop table if exists measures_etl_jobs;
create table measures_etl_jobs (
  job_id   bigint  not null,
  load_id  text    not null,
  value    integer not null,
  primary key (job_id, load_id)
);

-- A table for resuming measure extraction from the last successfully completed load operation.
drop table if exists measures_etl_progress;
create table measures_etl_progress (
  job_id          bigint primary key,
  last_measure_id bigint not null,
  dataset_type    smallint not null,
  job_ts          timestamptz default now()
);


-------------------------------
-- Measures GC
-- This supports periodic deletion of unstructured measures to remove redundant
-- data that has been loaded into the structured schema via our ETL process.
--
-- Note: we could create a generalized set of measures_async_{launch, jobs, progress}
-- tables that distinguish between ETL and GC jobs with a job_type field.
-- However we keep them separate for simpler dispatching and association of triggers.

-- The ETL launch table has triggers registered on it to perform GC functionality
-- in the background, similarly to our ETL tasks.
drop table if exists measures_gc_launch;
create table measures_gc_launch (
  job_id       bigserial primary key,
  dataset_type smallint not null,
  launch_ts    timestamptz default now()
);

-- A table for tracking deletion statistics.
drop table if exists measures_gc_progress;
create table measures_gc_progress (
  job_id          bigint primary key,
  min_measure_id  bigint not null,
  max_measure_id  bigint not null,
  dataset_type    smallint not null,
  job_ts          timestamptz default now()
);

create or replace function measures_mc_granola_gc(gc_job_id bigint, gc_delay interval) returns void as $$
begin
  -- Disable the deletion trigger, since we do not want to modify the historical averages.
  alter table mc_granola_measures disable trigger on_delete_mc_granola_measures;

  with deleted_ids as (
    delete from mc_granola_measures where created_at < (now() - gc_delay) returning id
  )
  insert into measures_gc_progress(job_id, min_measure_id, max_measure_id, dataset_type)
  select gc_job_id as job_id,
         coalesce(min(id), -1) as min_measure_id,
         coalesce(max(id), -1) as max_measure_id,
         (0::smallint) as dataset_type
  from deleted_ids;

  -- Re-enable the deletion trigger.
  alter table mc_granola_measures enable trigger on_delete_mc_granola_measures;
  return;
end;
$$
language plpgsql;

create or replace function measures_mc_json_gc(gc_job_id bigint, gc_delay interval) returns void as $$
begin
  -- Disable the deletion trigger, since we do not want to modify the historical averages.
  alter table mc_json_measures disable trigger on_delete_mc_json_measures;

  with deleted_ids as (
    delete from mc_json_measures where created_at < (now() - gc_delay) returning id
  )
  insert into measures_gc_progress(job_id, min_measure_id, max_measure_id, dataset_type)
  select gc_job_id as job_id,
         coalesce(min(id), -1) as min_measure_id,
         coalesce(max(id), -1) as max_measure_id,
         (1::smallint) as dataset_type
  from deleted_ids;

  -- Re-enable the deletion trigger.
  alter table mc_json_measures enable trigger on_delete_mc_json_measures;
  return;
end;
$$
language plpgsql;

-- TODO: handle gc_delay::interval cast error.
create or replace function mc_gc_launch_fn() returns trigger as $$
declare
  gc_delay interval;
begin
  select param_value::interval into gc_delay from mc_parameters where param_key = E'gc_delay';
  if NEW.dataset_type = 0 then
    perform measures_mc_granola_gc(NEW.job_id, gc_delay);
  elsif NEW.dataset_type = 1 then
    perform measures_mc_json_gc(NEW.job_id, gc_delay);
  end if;

  -- Return values of after trigger are ignored.
  return null;
end;
$$
language plpgsql;

drop trigger if exists on_insert_measures_gc_launch on measures_gc_launch;

create trigger on_insert_measures_gc_launch
after insert on measures_gc_launch
for each row execute procedure mc_gc_launch_fn();
