--------------------------------------
-- Metabolic Compass measures tables.
--
-- These are populated from the 'measures' table implementing a JSON queue.
-- These tables depend on mc_measures_seq for their primary key.

drop table if exists mc_body_measures;
drop table if exists mc_blood_pressure_measures;
drop table if exists mc_sleep_measures;
drop table if exists mc_meal_measures;
drop table if exists mc_activity_measures;
drop table if exists mc_light_measures;
drop table if exists mc_energy_measures;
drop table if exists mc_blood_measures;
drop table if exists mc_lung_measures;
drop table if exists mc_heart_rate_measures;
drop table if exists mc_nutrients_macro_measures;
drop table if exists mc_nutrients_subsets_measures;
drop table if exists mc_nutrients_minerals_measures;
drop table if exists mc_nutrients_vitamins_measures;
drop table if exists mc_nutrients_liquids_measures;
drop table if exists mc_misc_measures;
drop table if exists mc_ongoing_events;

create table mc_body_measures (
  sid                    bigint primary key,
  udbid                  bigint,
  ts                     timestamptz,
  uuid                   uuid,
  body_weight            real,
  body_height            real,
  body_mass_index        real,
  body_fat_percentage    real,
  body_temperature       real,
  basal_body_temperature real,
  lean_body_mass         real,
  metadata               jsonb
);

create table mc_blood_pressure_measures (
  sid                       bigint primary key,
  udbid                     bigint,
  ts                        timestamptz,
  uuid                      uuid,
  systolic_blood_pressure   real,
  diastolic_blood_pressure  real,
  metadata                  jsonb
);

create table mc_sleep_measures (
  sid             bigint primary key,
  udbid           bigint,
  ts              timestamptz,
  uuid            uuid,
  sleep_duration  real,
  metadata        jsonb
);

create table mc_meal_measures (
  sid           bigint primary key,
  udbid         bigint,
  ts            timestamptz,
  uuid          uuid,
  meal_duration real,
  food_type     jsonb,
  metadata      jsonb
);

create table mc_activity_measures (
  sid               bigint primary key,
  udbid             bigint,
  ts                timestamptz,
  uuid              uuid,
  activity_duration real,
  activity_type     integer,
  activity_value    jsonb,
  metadata          jsonb
);

create table mc_light_measures (
  sid           bigint primary key,
  udbid         bigint,
  ts            timestamptz,
  uuid          uuid,
  uv_exposure   real,
  metadata      jsonb
);

create table mc_energy_measures (
  sid                   bigint primary key,
  udbid                 bigint,
  ts                    timestamptz,
  uuid                  uuid,
  active_energy_burned  real,
  basal_energy_burned   real,
  metadata              jsonb
);

create table mc_blood_measures (
  sid                     bigint primary key,
  udbid                   bigint,
  ts                      timestamptz,
  uuid                    uuid,
  blood_alcohol_content   real,
  blood_glucose           real,
  blood_oxygen_saturation real,
  metadata                jsonb
);

create table mc_lung_measures (
  sid                                   bigint primary key,
  udbid                                 bigint,
  ts                                    timestamptz,
  uuid                                  uuid,
  forced_expiratory_volume_one_second   real,
  forced_vital_capacity                 real,
  peak_expiratory_flow                  real,
  inhaler_usage                         real,
  respiratory_rate                      real,
  metadata                              jsonb
);

create table mc_heart_rate_measures (
  sid         bigint primary key,
  udbid       bigint,
  ts          timestamptz,
  uuid        uuid,
  heart_rate  real,
  metadata    jsonb
);

create table mc_nutrients_macro_measures (
  sid                         bigint primary key,
  udbid                       bigint,
  ts                          timestamptz,
  uuid                        uuid,
  dietary_carbohydrates       real,
  dietary_energy_consumed     real,
  dietary_fat_total           real,
  dietary_protein             real,
  metadata                    jsonb
);

create table mc_nutrients_subsets_measures (
  sid                         bigint primary key,
  udbid                       bigint,
  ts                          timestamptz,
  uuid                        uuid,
  dietary_caffeine            real,
  dietary_cholesterol         real,
  dietary_fat_monounsaturated real,
  dietary_fat_polyunsaturated real,
  dietary_fat_saturated       real,
  dietary_fiber               real,
  dietary_sugar               real,
  metadata                    jsonb
);

create table mc_nutrients_minerals_measures (
  sid                         bigint primary key,
  udbid                       bigint,
  ts                          timestamptz,
  uuid                        uuid,
  dietary_calcium             real,
  dietary_chloride            real,
  dietary_chromium            real,
  dietary_copper              real,
  dietary_iodine              real,
  dietary_iron                real,
  dietary_magnesium           real,
  dietary_manganese           real,
  dietary_molybdenum          real,
  dietary_niacin              real,
  dietary_phosphorus          real,
  dietary_potassium           real,
  dietary_selenium            real,
  dietary_sodium              real,
  dietary_zinc                real,
  metadata                    jsonb
);

create table mc_nutrients_vitamins_measures (
  sid                         bigint primary key,
  udbid                       bigint,
  ts                          timestamptz,
  uuid                        uuid,
  dietary_biotin              real,
  dietary_folate              real,
  dietary_pantothenic_acid    real,
  dietary_riboflavin          real,
  dietary_thiamin             real,
  dietary_vitamina            real,
  dietary_vitaminb12          real,
  dietary_vitaminb6           real,
  dietary_vitaminc            real,
  dietary_vitamind            real,
  dietary_vitamine            real,
  dietary_vitamink            real,
  metadata                    jsonb
);

create table mc_nutrients_liquids_measures (
  sid                         bigint primary key,
  udbid                       bigint,
  ts                          timestamptz,
  uuid                        uuid,
  dietary_alcohol             real,
  dietary_water               real,
  metadata                    jsonb
);

create table mc_misc_measures (
  sid                         bigint primary key,
  udbid                       bigint,
  ts                          timestamptz,
  uuid                        uuid,
  apple_stand_hour            real,
  electrodermal_activity      real,
  nike_fuel                   real,
  number_of_times_fallen      real,
  peripheral_perfusion_index  real,
  metadata                    jsonb
);

----------------------------------------
-- Interval construction.

-- A table for ongoing events, whose ending times have
-- yet to be defined.
create table mc_ongoing_events (
  id          bytea,
  ts          timestamptz,
  event_type  int,
  metadata    jsonb,
  primary key (id, ts)
);

--------------------------------------------
-- Measures indices.

create index idx_mc_body_measures_ts               on mc_body_measures               using btree(ts);
create index idx_mc_blood_pressure_measures_ts     on mc_blood_pressure_measures     using btree(ts);
create index idx_mc_sleep_measures_ts              on mc_sleep_measures              using btree(ts);
create index idx_mc_meal_measures_ts               on mc_meal_measures               using btree(ts);
create index idx_mc_activity_measures_ts           on mc_activity_measures           using btree(ts);
create index idx_mc_light_measures_ts              on mc_light_measures              using btree(ts);
create index idx_mc_energy_measures_ts             on mc_energy_measures             using btree(ts);
create index idx_mc_blood_measures_ts              on mc_blood_measures              using btree(ts);
create index idx_mc_lung_measures_ts               on mc_lung_measures               using btree(ts);
create index idx_mc_heart_rate_measures_ts         on mc_heart_rate_measures         using btree(ts);
create index idx_mc_nutrients_macro_measures_ts    on mc_nutrients_macro_measures    using btree(ts);
create index idx_mc_nutrients_subsets_measures_ts  on mc_nutrients_subsets_measures  using btree(ts);
create index idx_mc_nutrients_minerals_measures_ts on mc_nutrients_minerals_measures using btree(ts);
create index idx_mc_nutrients_vitamins_measures_ts on mc_nutrients_vitamins_measures using btree(ts);
create index idx_mc_nutrients_liquids_measures_ts  on mc_nutrients_liquids_measures  using btree(ts);
create index idx_mc_misc_measures_ts               on mc_misc_measures               using btree(ts);
create index idx_mc_ongoing_events_ts              on mc_ongoing_events              using btree(ts);

create index idx_mc_body_measures_uuid               on mc_body_measures               using btree(uuid) where uuid is not null;
create index idx_mc_blood_pressure_measures_uuid     on mc_blood_pressure_measures     using btree(uuid) where uuid is not null;
create index idx_mc_sleep_measures_uuid              on mc_sleep_measures              using btree(uuid) where uuid is not null;
create index idx_mc_meal_measures_uuid               on mc_meal_measures               using btree(uuid) where uuid is not null;
create index idx_mc_activity_measures_uuid           on mc_activity_measures           using btree(uuid) where uuid is not null;
create index idx_mc_light_measures_uuid              on mc_light_measures              using btree(uuid) where uuid is not null;
create index idx_mc_energy_measures_uuid             on mc_energy_measures             using btree(uuid) where uuid is not null;
create index idx_mc_blood_measures_uuid              on mc_blood_measures              using btree(uuid) where uuid is not null;
create index idx_mc_lung_measures_uuid               on mc_lung_measures               using btree(uuid) where uuid is not null;
create index idx_mc_heart_rate_measures_uuid         on mc_heart_rate_measures         using btree(uuid) where uuid is not null;
create index idx_mc_nutrients_macro_measures_uuid    on mc_nutrients_macro_measures    using btree(uuid) where uuid is not null;
create index idx_mc_nutrients_subsets_measures_uuid  on mc_nutrients_subsets_measures  using btree(uuid) where uuid is not null;
create index idx_mc_nutrients_minerals_measures_uuid on mc_nutrients_minerals_measures using btree(uuid) where uuid is not null;
create index idx_mc_nutrients_vitamins_measures_uuid on mc_nutrients_vitamins_measures using btree(uuid) where uuid is not null;
create index idx_mc_nutrients_liquids_measures_uuid  on mc_nutrients_liquids_measures  using btree(uuid) where uuid is not null;
create index idx_mc_misc_measures_uuid               on mc_misc_measures               using btree(uuid) where uuid is not null;


----------------------------------------
-- Activity codes.
--
create or replace function mc_activity_code(hk_activity text) returns integer as
$$
  var hk_activity_types = {
    'HKWorkoutActivityTypeAmericanFootball'             : 'american_football'               ,
    'HKWorkoutActivityTypeArchery'                      : 'archery'                         ,
    'HKWorkoutActivityTypeAustralianFootball'           : 'australian_football'             ,
    'HKWorkoutActivityTypeBadminton'                    : 'badminton'                       ,
    'HKWorkoutActivityTypeBaseball'                     : 'baseball'                        ,
    'HKWorkoutActivityTypeBasketball'                   : 'basketball'                      ,
    'HKWorkoutActivityTypeBowling'                      : 'bowling'                         ,
    'HKWorkoutActivityTypeBoxing'                       : 'boxing'                          ,
    'HKWorkoutActivityTypeClimbing'                     : 'climbing'                        ,
    'HKWorkoutActivityTypeCricket'                      : 'cricket'                         ,
    'HKWorkoutActivityTypeCrossTraining'                : 'cross_training'                  ,
    'HKWorkoutActivityTypeCurling'                      : 'curling'                         ,
    'HKWorkoutActivityTypeCycling'                      : 'cycling'                         ,
    'HKWorkoutActivityTypeDance'                        : 'dance'                           ,
    'HKWorkoutActivityTypeDanceInspiredTraining'        : 'dance_inspired_training'         ,
    'HKWorkoutActivityTypeElliptical'                   : 'elliptical'                      ,
    'HKWorkoutActivityTypeEquestrianSports'             : 'equestrian_sports'               ,
    'HKWorkoutActivityTypeFencing'                      : 'fencing'                         ,
    'HKWorkoutActivityTypeFishing'                      : 'fishing'                         ,
    'HKWorkoutActivityTypeFunctionalStrengthTraining'   : 'functional_strength_training'    ,
    'HKWorkoutActivityTypeGolf'                         : 'golf'                            ,
    'HKWorkoutActivityTypeGymnastics'                   : 'gymnastics'                      ,
    'HKWorkoutActivityTypeHandball'                     : 'handball'                        ,
    'HKWorkoutActivityTypeHiking'                       : 'hiking'                          ,
    'HKWorkoutActivityTypeHockey'                       : 'hockey'                          ,
    'HKWorkoutActivityTypeHunting'                      : 'hunting'                         ,
    'HKWorkoutActivityTypeLacrosse'                     : 'lacrosse'                        ,
    'HKWorkoutActivityTypeMartialArts'                  : 'martial_arts'                    ,
    'HKWorkoutActivityTypeMindAndBody'                  : 'mind_and_body'                   ,
    'HKWorkoutActivityTypeMixedMetabolicCardioTraining' : 'mixed_metabolic_cardio_training' ,
    'HKWorkoutActivityTypePaddleSports'                 : 'paddle_sports'                   ,
    'HKWorkoutActivityTypePlay'                         : 'play'                            ,
    'HKWorkoutActivityTypePreparationAndRecovery'       : 'preparation_and_recovery'        ,
    'HKWorkoutActivityTypeRacquetball'                  : 'racquetball'                     ,
    'HKWorkoutActivityTypeRowing'                       : 'rowing'                          ,
    'HKWorkoutActivityTypeRugby'                        : 'rugby'                           ,
    'HKWorkoutActivityTypeRunning'                      : 'running'                         ,
    'HKWorkoutActivityTypeSailing'                      : 'sailing'                         ,
    'HKWorkoutActivityTypeSkatingSports'                : 'skating_sports'                  ,
    'HKWorkoutActivityTypeSnowSports'                   : 'snow_sports'                     ,
    'HKWorkoutActivityTypeSoccer'                       : 'soccer'                          ,
    'HKWorkoutActivityTypeSoftball'                     : 'softball'                        ,
    'HKWorkoutActivityTypeSquash'                       : 'squash'                          ,
    'HKWorkoutActivityTypeStairClimbing'                : 'stair_climbing'                  ,
    'HKWorkoutActivityTypeSurfingSports'                : 'surfing_sports'                  ,
    'HKWorkoutActivityTypeSwimming'                     : 'swimming'                        ,
    'HKWorkoutActivityTypeTableTennis'                  : 'table_tennis'                    ,
    'HKWorkoutActivityTypeTennis'                       : 'tennis'                          ,
    'HKWorkoutActivityTypeTrackAndField'                : 'track_and_field'                 ,
    'HKWorkoutActivityTypeTraditionalStrengthTraining'  : 'traditional_strength_training'   ,
    'HKWorkoutActivityTypeVolleyball'                   : 'volleyball'                      ,
    'HKWorkoutActivityTypeWalking'                      : 'walking'                         ,
    'HKWorkoutActivityTypeWaterFitness'                 : 'water_fitness'                   ,
    'HKWorkoutActivityTypeWaterPolo'                    : 'water_polo'                      ,
    'HKWorkoutActivityTypeWaterSports'                  : 'water_sports'                    ,
    'HKWorkoutActivityTypeWrestling'                    : 'wrestling'                       ,
    'HKWorkoutActivityTypeYoga'                         : 'yoga'                            ,
    'HKWorkoutActivityTypeOther'                        : 'other'                           ,
    'HKQuantityTypeIdentifierStepCount'                 : 'step_count'                      ,
    'HKQuantityTypeIdentifierDistanceWalkingRunning'    : 'distance_walking_running'        ,
    'HKQuantityTypeIdentifierFlightsClimbed'            : 'flights_climbed'
  };

  var mc_activity_types = {
    'american_football'               : 1    ,
    'archery'                         : 2    ,
    'australian_football'             : 3    ,
    'badminton'                       : 4    ,
    'baseball'                        : 5    ,
    'basketball'                      : 6    ,
    'bowling'                         : 7    ,
    'boxing'                          : 8    ,
    'climbing'                        : 9    ,
    'cricket'                         : 10   ,
    'cross_training'                  : 11   ,
    'curling'                         : 12   ,
    'cycling'                         : 13   ,
    'dance'                           : 14   ,
    'dance_inspired_training'         : 15   ,
    'elliptical'                      : 16   ,
    'equestrian_sports'               : 17   ,
    'fencing'                         : 18   ,
    'fishing'                         : 19   ,
    'functional_strength_training'    : 20   ,
    'golf'                            : 21   ,
    'gymnastics'                      : 22   ,
    'handball'                        : 23   ,
    'hiking'                          : 24   ,
    'hockey'                          : 25   ,
    'hunting'                         : 26   ,
    'lacrosse'                        : 27   ,
    'martial_arts'                    : 28   ,
    'mind_and_body'                   : 29   ,
    'mixed_metabolic_cardio_training' : 30   ,
    'paddle_sports'                   : 31   ,
    'play'                            : 32   ,
    'preparation_and_recovery'        : 33   ,
    'racquetball'                     : 34   ,
    'rowing'                          : 35   ,
    'rugby'                           : 36   ,
    'running'                         : 37   ,
    'sailing'                         : 38   ,
    'skating_sports'                  : 39   ,
    'snow_sports'                     : 40   ,
    'soccer'                          : 41   ,
    'softball'                        : 42   ,
    'squash'                          : 43   ,
    'stair_climbing'                  : 44   ,
    'surfing_sports'                  : 45   ,
    'swimming'                        : 46   ,
    'table_tennis'                    : 47   ,
    'tennis'                          : 48   ,
    'track_and_field'                 : 49   ,
    'traditional_strength_training'   : 50   ,
    'volleyball'                      : 51   ,
    'walking'                         : 52   ,
    'water_fitness'                   : 53   ,
    'water_polo'                      : 54   ,
    'water_sports'                    : 55   ,
    'wrestling'                       : 56   ,
    'yoga'                            : 57   ,
    'other'                           : 3000 ,
    'step_count'                      : 58   ,
    'distance_walking_running'        : 59   ,
    'flights_climbed'                 : 60
  };

  if ( hk_activity_types.hasOwnProperty(hk_activity) ) {
    var mc_activity = hk_activity_types[hk_activity];
    if ( mc_activity_types.hasOwnProperty(mc_activity) ) {
      return mc_activity_types[mc_activity];
    }
  }
  return -1;
$$
language plv8;
