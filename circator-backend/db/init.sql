------------------------------
-- PG components.
create extension plv8;

-----------------------------
-- Metabolic compass users

drop table if exists users;

-- Unique internal user ids.
-- This is based on: http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
drop sequence if exists user_id_seq;
create sequence user_id_seq;

create or replace function next_user_id(out result bigint) as $$
declare
    our_epoch bigint := 1314220021721;
    seq_id bigint;
    now_millis bigint;
    shard_id int := 5;
begin
    select nextval('user_id_seq') % 1024 into seq_id;

    select floor(extract(epoch from clock_timestamp()) * 1000) into now_millis;
    result := (now_millis - our_epoch) << 23;
    result := result | (shard_id << 10);
    result := result | (seq_id);
end;
$$ language plpgsql;

-- Users table.
-- This is anonymized and contains no personally identifiable information.
-- The 'id' field represents an identifier obtained from a third party
-- (e.g., an account id hash, or OAuth2 token from Stormpath or Twitter)

drop table if exists users;
create table users (
  id            bytea   primary key,
  udbid         bigint  not null default next_user_id(),
  profile       jsonb   not null,
  settings      jsonb,
  archive_span  jsonb,
  last_acquired jsonb
);

-- Index users table on udbid (btree) since this is used throughout other
-- database tables, and on profile (GIN) for path-oriented profile queries.
create unique index idx_users_udbid on users(udbid);
create index idx_users_profile on users using GIN (profile jsonb_path_ops);


--------------------------------
-- Stormpath/MC user id mapping.

drop table if exists mcsp_user_mappings;
create table mcsp_user_mappings (
  sp_id     varchar(32),
  mc_id     bytea
);

-----------------------------
-- Metabolic compass dataset

-- Measures sequence, for a total ordering over both mc_granola_measures and mc_json_measures
drop sequence if exists mc_measures_seq;
create sequence mc_measures_seq;

-- Measures queues, in Granola format, and MC format.
-- This uses the same definition as massive-js/lib/scripts/create_document_table.sql

drop table if exists mc_granola_measures;
drop table if exists mc_json_measures;

-- Measures in Granola JSON format.
-- See the OpenMHealth / Granola schemas:
-- http://www.openmhealth.org/documentation/#/schema-docs/overview
create table mc_granola_measures (
  id         bigint primary key default nextval('mc_measures_seq'),
  body       jsonb not null,
  search     tsvector,
  created_at timestamptz default now()
);

create unique index idx_mc_granola_measures_uuid on mc_granola_measures using btree(((body#>>'{header, id}')::uuid));
create index idx_mc_granola_measures on mc_granola_measures using GIN(body jsonb_path_ops);
create index idx_search_mc_granola_measures on mc_granola_measures using GIN(search);

-- Measures in MC JSON format.
-- See our schemas here:
-- https://www.metaboliccompass.com/schemas/measure.json
create table mc_json_measures (
  id         bigint primary key default nextval('mc_measures_seq'),
  rel        text not null,
  body       jsonb not null,
  userid     text not null,
  search     tsvector,
  created_at timestamptz default now()
);

create index idx_mc_json_measures on mc_json_measures using GIN(body jsonb_path_ops);
create index idx_search_mc_json_measures on mc_json_measures using GIN(search);


--------------------------------------
-- Workflow configuration parameters,
-- as a pair of key-value strings.

drop table if exists mc_parameters;
create table mc_parameters (
  param_key    text primary key,
  param_value  text
);

-- Static parameters.
insert into mc_parameters values (E'etl_batch_size', 10000::text);
insert into mc_parameters values (E'gc_delay', '12 months'::text);


--------------------------------------------
-- JSON utility functions.

-- JSON object merging for deduplication.
create or replace function jsonb_append(jsonb, jsonb) returns jsonb AS $$
  with json_union as (
    select * from jsonb_each($1)
    union all
    select * from jsonb_each($2)
  )
  select json_object_agg(key, value)::jsonb from json_union;
$$ language sql;

drop aggregate if exists jsonb_merge(jsonb);
create aggregate jsonb_merge (jsonb) (
  sfunc = jsonb_append,
  stype = jsonb,
  initcond = '{}'
);
