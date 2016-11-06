--
-- Incremental view maintenance for Metabolic Compass population queries.
--
-- This will perform IVM for average queries only.
-- For more complex maintenance, use DBToaster.

-- TODO: overflow?
drop table if exists mc_sumcount_by_user;
create table mc_sumcount_by_user (
  msid    int,
  udbid   bigint,
  msum    double precision,
  mcnt    bigint,
  primary key (msid, udbid)
);

drop table if exists mc_sumcount_by_user_day;
create table mc_sumcount_by_user_day (
  msid    int,
  udbid   bigint,
  day     timestamptz,
  msum    double precision,
  mcnt    bigint,
  primary key (msid, udbid, day)
);

drop table if exists mc_vm_user_windows;
create table mc_vm_user_windows (
  msid     int,
  udbid    bigint,
  start_ts timestamptz,
  primary key (msid, udbid)
);

-- Trigger code to perform maintenance.
create or replace function mc_granola_measures_sumcount_vm_fn() returns trigger as
$$
  // Utilities
  var objIsEmpty = function(o) { return Object.keys(o).length == 0; };
  var strIsEmpty = function(s) { return (!s || s === ''); };

  // Extractors.
  var duration_extractor = function(v) {
    return v.duration.value;
  };

  var duration_ts_extractor = function(v) {
    if ( v.effective_time_frame.hasOwnProperty('time_interval') ) {
      var endDate = Date.parse(v.effective_time_frame.time_interval.end_date_time);
      var startDate = Date.parse(v.effective_time_frame.time_interval.start_date_time);
      return (endDate - startDate) / 1000;
    } else {
      plv8.elog(WARNING, 'No valid duration in', JSON.stringify(v));
      return 0;
    }
  };

  var namedjson_extractor = function(name, v) {
    return v[name];
  };

  // Note that distance/walking/running activities contribute to the glboal distance aggregate measure.
  var distance_walking_running_activity = {
    type_id: 10059,
    duration: duration_ts_extractor,
    vals: [{
      id: 20059,
      value: function(v) { return v.unit_value.value; },
    }, {
      id: 50001,
      value: function(v) { return v.unit_value.value; },
    }]
  };

  var flights_climbed_activity = {
    type_id: 10060,
    duration: function(v) { return 0; },
    vals: [{
      id: 20060,
      value: function(v) { return v.count; },
    }]
  };

  var step_count_activity = {
    type_id: 10058,
    duration: duration_ts_extractor,
    vals: [{
      id: 20058,
      value: namedjson_extractor.bind(undefined, 'step_count')
    }]
  };

  var mc_attrs_acquisition_class = {
    'body_weight'                         : {cumulative: false },
    'body_height'                         : {cumulative: false },
    'body_mass_index'                     : {cumulative: false },
    'body_fat_percentage'                 : {cumulative: false },
    'body_temperature'                    : {cumulative: false },
    'basal_body_temperature'              : {cumulative: false },
    'lean_body_mass'                      : {cumulative: false },
    'systolic_blood_pressure'             : {cumulative: false },
    'diastolic_blood_pressure'            : {cumulative: false },
    'sleep_duration'                      : {cumulative: false },
    'meal_duration'                       : {cumulative: true  },  // With cumulative meal durations, we sum up the durations per day (optionally by type)
    'food_type'                           : {cumulative: false },
    'activity_duration'                   : {cumulative: true  },  // With cumulative activity durations, we sum up the durations per day (optionally by type)
    'activity_type'                       : {cumulative: false },
    'activity_value'                      : {cumulative: true  },  // With cumulative activity values, we sum up the activity quantities per day (optionally by type).
    'uv_exposure'                         : {cumulative: false },
    'active_energy_burned'                : {cumulative: true  },
    'basal_energy_burned'                 : {cumulative: true  },
    'blood_alcohol_content'               : {cumulative: false },
    'blood_glucose'                       : {cumulative: false },
    'blood_oxygen_saturation'             : {cumulative: false },
    'forced_expiratory_volume_one_second' : {cumulative: false },
    'forced_vital_capacity'               : {cumulative: false },
    'peak_expiratory_flow'                : {cumulative: false },
    'inhaler_usage'                       : {cumulative: true  },
    'respiratory_rate'                    : {cumulative: false },
    'heart_rate'                          : {cumulative: false },
    'dietary_carbohydrates'               : {cumulative: true  },
    'dietary_energy_consumed'             : {cumulative: true  },
    'dietary_fat_total'                   : {cumulative: true  },
    'dietary_protein'                     : {cumulative: true  },
    'dietary_caffeine'                    : {cumulative: true  },
    'dietary_cholesterol'                 : {cumulative: true  },
    'dietary_fat_monounsaturated'         : {cumulative: true  },
    'dietary_fat_polyunsaturated'         : {cumulative: true  },
    'dietary_fat_saturated'               : {cumulative: true  },
    'dietary_fiber'                       : {cumulative: true  },
    'dietary_sugar'                       : {cumulative: true  },
    'dietary_calcium'                     : {cumulative: true  },
    'dietary_chloride'                    : {cumulative: true  },
    'dietary_chromium'                    : {cumulative: true  },
    'dietary_copper'                      : {cumulative: true  },
    'dietary_iodine'                      : {cumulative: true  },
    'dietary_iron'                        : {cumulative: true  },
    'dietary_magnesium'                   : {cumulative: true  },
    'dietary_manganese'                   : {cumulative: true  },
    'dietary_molybdenum'                  : {cumulative: true  },
    'dietary_niacin'                      : {cumulative: true  },
    'dietary_phosphorus'                  : {cumulative: true  },
    'dietary_potassium'                   : {cumulative: true  },
    'dietary_selenium'                    : {cumulative: true  },
    'dietary_sodium'                      : {cumulative: true  },
    'dietary_zinc'                        : {cumulative: true  },
    'dietary_biotin'                      : {cumulative: true  },
    'dietary_folate'                      : {cumulative: true  },
    'dietary_pantothenic_acid'            : {cumulative: true  },
    'dietary_riboflavin'                  : {cumulative: true  },
    'dietary_thiamin'                     : {cumulative: true  },
    'dietary_vitamina'                    : {cumulative: true  },
    'dietary_vitaminb12'                  : {cumulative: true  },
    'dietary_vitaminb6'                   : {cumulative: true  },
    'dietary_vitaminc'                    : {cumulative: true  },
    'dietary_vitamind'                    : {cumulative: true  },
    'dietary_vitamine'                    : {cumulative: true  },
    'dietary_vitamink'                    : {cumulative: true  },
    'dietary_alcohol'                     : {cumulative: true  },
    'dietary_water'                       : {cumulative: true  },
    'apple_stand_hour'                    : {cumulative: false },
    'electrodermal_activity'              : {cumulative: false },
    'nike_fuel'                           : {cumulative: true  },
    'number_of_times_fallen'              : {cumulative: true  },
    'peripheral_perfusion_index'          : {cumulative: false },
  };

  var quantityToMCDB = {
    'HKQuantityTypeIdentifierActiveEnergyBurned'        : {rel: 'mc_energy_measures'             , attr: 'active_energy_burned'                , id: 16 },
    'HKQuantityTypeIdentifierBasalEnergyBurned'         : {rel: 'mc_energy_measures'             , attr: 'basal_energy_burned'                 , id: 17 },
    'HKQuantityTypeIdentifierBloodAlcoholContent'       : {rel: 'mc_blood_measures'              , attr: 'blood_alcohol_content'               , id: 18 },
    'HKQuantityTypeIdentifierBloodPressureDiastolic'    : {rel: 'mc_blood_pressure_measures'     , attr: 'diastolic_blood_pressure'            , id: 7 },
    'HKQuantityTypeIdentifierBloodPressureSystolic'     : {rel: 'mc_blood_pressure_measures'     , attr: 'systolic_blood_pressure'             , id: 8 },
    'HKQuantityTypeIdentifierDietaryAlcohol'            : {rel: 'mc_nutrients_liquids_measures'  , attr: 'dietary_alcohol'                     , id: 65 },
    'HKQuantityTypeIdentifierDietaryBiotin'             : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_biotin'                      , id: 53 },
    'HKQuantityTypeIdentifierDietaryCaffeine'           : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_caffeine'                    , id: 31 },
    'HKQuantityTypeIdentifierDietaryCalcium'            : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_calcium'                     , id: 38 },
    'HKQuantityTypeIdentifierDietaryCarbohydrates'      : {rel: 'mc_nutrients_macro_measures'    , attr: 'dietary_carbohydrates'               , id: 27 },
    'HKQuantityTypeIdentifierDietaryChloride'           : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_chloride'                    , id: 39 },
    'HKQuantityTypeIdentifierDietaryCholesterol'        : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_cholesterol'                 , id: 32},
    'HKQuantityTypeIdentifierDietaryChromium'           : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_chromium'                    , id: 40 },
    'HKQuantityTypeIdentifierDietaryCopper'             : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_copper'                      , id: 41 },
    'HKQuantityTypeIdentifierDietaryEnergyConsumed'     : {rel: 'mc_nutrients_macro_measures'    , attr: 'dietary_energy_consumed'             , id: 28 },
    'HKQuantityTypeIdentifierDietaryFatMonounsaturated' : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_fat_monounsaturated'         , id: 33 },
    'HKQuantityTypeIdentifierDietaryFatPolyunsaturated' : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_fat_polyunsaturated'         , id: 34 },
    'HKQuantityTypeIdentifierDietaryFatSaturated'       : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_fat_saturated'               , id: 35 },
    'HKQuantityTypeIdentifierDietaryFatTotal'           : {rel: 'mc_nutrients_macro_measures'    , attr: 'dietary_fat_total'                   , id: 29 },
    'HKQuantityTypeIdentifierDietaryFiber'              : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_fiber'                       , id: 36 },
    'HKQuantityTypeIdentifierDietaryFolate'             : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_folate'                      , id: 54 },
    'HKQuantityTypeIdentifierDietaryIodine'             : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_iodine'                      , id: 42 },
    'HKQuantityTypeIdentifierDietaryIron'               : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_iron'                        , id: 43 },
    'HKQuantityTypeIdentifierDietaryMagnesium'          : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_magnesium'                   , id: 44 },
    'HKQuantityTypeIdentifierDietaryManganese'          : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_manganese'                   , id: 45 },
    'HKQuantityTypeIdentifierDietaryMolybdenum'         : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_molybdenum'                  , id: 46 },
    'HKQuantityTypeIdentifierDietaryNiacin'             : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_niacin'                      , id: 47 },
    'HKQuantityTypeIdentifierDietaryPantothenicAcid'    : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_pantothenic_acid'            , id: 55 },
    'HKQuantityTypeIdentifierDietaryPhosphorus'         : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_phosphorus'                  , id: 48 },
    'HKQuantityTypeIdentifierDietaryPotassium'          : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_potassium'                   , id: 49 },
    'HKQuantityTypeIdentifierDietaryProtein'            : {rel: 'mc_nutrients_macro_measures'    , attr: 'dietary_protein'                     , id: 30 },
    'HKQuantityTypeIdentifierDietaryRiboflavin'         : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_riboflavin'                  , id: 56 },
    'HKQuantityTypeIdentifierDietarySelenium'           : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_selenium'                    , id: 50 },
    'HKQuantityTypeIdentifierDietarySodium'             : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_sodium'                      , id: 51 },
    'HKQuantityTypeIdentifierDietaryThiamin'            : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_thiamin'                     , id: 57 },
    'HKQuantityTypeIdentifierDietarySugar'              : {rel: 'mc_nutrients_subsets_measures'  , attr: 'dietary_sugar'                       , id: 37 },
    'HKQuantityTypeIdentifierDietaryVitaminA'           : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitamina'                    , id: 58 },
    'HKQuantityTypeIdentifierDietaryVitaminB12'         : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitaminb12'                  , id: 59 },
    'HKQuantityTypeIdentifierDietaryVitaminB6'          : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitaminb6'                   , id: 60 },
    'HKQuantityTypeIdentifierDietaryVitaminC'           : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitaminc'                    , id: 61 },
    'HKQuantityTypeIdentifierDietaryVitaminD'           : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitamind'                    , id: 62 },
    'HKQuantityTypeIdentifierDietaryVitaminE'           : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitamine'                    , id: 63 },
    'HKQuantityTypeIdentifierDietaryVitaminK'           : {rel: 'mc_nutrients_vitamins_measures' , attr: 'dietary_vitamink'                    , id: 64 },
    'HKQuantityTypeIdentifierDietaryWater'              : {rel: 'mc_nutrients_liquids_measures'  , attr: 'dietary_water'                       , id: 66 },
    'HKQuantityTypeIdentifierDietaryZinc'               : {rel: 'mc_nutrients_minerals_measures' , attr: 'dietary_zinc'                        , id: 52 },
    'HKQuantityTypeIdentifierDistanceWalkingRunning'    : {rel: 'mc_activity_measures'           , activity: distance_walking_running_activity          },
    'HKQuantityTypeIdentifierElectrodermalActivity'     : {rel: 'mc_misc_measures'               , attr: 'electrodermal_activity'              , id: 68 },
    'HKQuantityTypeIdentifierFlightsClimbed'            : {rel: 'mc_activity_measures'           , activity: flights_climbed_activity                   },
    'HKQuantityTypeIdentifierForcedExpiratoryVolume1'   : {rel: 'mc_lung_measures'               , attr: 'forced_expiratory_volume_one_second' , id: 21 },
    'HKQuantityTypeIdentifierForcedVitalCapacity'       : {rel: 'mc_lung_measures'               , attr: 'forced_vital_capacity'               , id: 22 },
    'HKQuantityTypeIdentifierHeartRate'                 : {rel: 'mc_heart_rate_measures'         , attr: 'heart_rate'                          , id: 26 },
    'HKQuantityTypeIdentifierHeight'                    : {rel: 'mc_body_measures'               , attr: 'body_height'                         , id: 1  },
    'HKQuantityTypeIdentifierInhalerUsage'              : {rel: 'mc_lung_measures'               , attr: 'inhaler_usage'                       , id: 24, count: true },
    'HKQuantityTypeIdentifierLeanBodyMass'              : {rel: 'mc_body_measures'               , attr: 'lean_body_mass'                      , id: 6  },
    'HKQuantityTypeIdentifierNikeFuel'                  : {rel: 'mc_misc_measures'               , attr: 'nike_fuel'                           , id: 69, count: true },
    'HKQuantityTypeIdentifierNumberOfTimesFallen'       : {rel: 'mc_misc_measures'               , attr: 'number_of_times_fallen'              , id: 70, count: true },
    'HKQuantityTypeIdentifierOxygenSaturation'          : {rel: 'mc_blood_measures'              , attr: 'blood_oxygen_saturation'             , id: 20 },
    'HKQuantityTypeIdentifierPeakExpiratoryFlowRate'    : {rel: 'mc_lung_measures'               , attr: 'peak_expiratory_flow'                , id: 23 },
    'HKQuantityTypeIdentifierPeripheralPerfusionIndex'  : {rel: 'mc_misc_measures'               , attr: 'peripheral_perfusion_index'          , id: 71 },
    'HKQuantityTypeIdentifierRespiratoryRate'           : {rel: 'mc_lung_measures'               , attr: 'respiratory_rate'                    , id: 25 },
    'HKQuantityTypeIdentifierUVExposure'                : {rel: 'mc_light_measures'              , attr: 'uv_exposure'                         , id: 15, count: true }
  };

  var categoryToMCDB = {
    'HKCategoryTypeIdentifierAppleStandHour' : {
      rel             : 'mc_misc_measures',
      attr            : 'apple_stand_hour',
      id              : 67,
      category_value  : "Standing",
      value_extractor : duration_ts_extractor
    }
  };

  var granolaToMCDB = {
    'blood_glucose'            : {rel: 'mc_blood_measures',          attr: 'blood_glucose'                     , id: 19 },
    'blood_pressure'           : {rel: 'mc_blood_pressure_measures', attrs: [{attr: 'diastolic_blood_pressure' , id: 7  },
                                                                             {attr: 'systolic_blood_pressure'  , id: 8  }]},
    'body_fat_percentage'      : {rel: 'mc_body_measures',           attr: 'body_fat_percentage'               , id: 3  },
    'body_height'              : {rel: 'mc_body_measures',           attr: 'body_height'                       , id: 1  },
    'body_mass_index'          : {rel: 'mc_body_measures',           attr: 'body_mass_index'                   , id: 2  },
    'body_temperature'         : {rel: 'mc_body_measures',           attr: 'body_temperature'                  , id: 4  },
    'body_weight'              : {rel: 'mc_body_measures',           attr: 'body_weight'                       , id: 0  },
    'diastolic_blood_pressure' : {rel: 'mc_blood_pressure_measures', attr: 'diastolic_blood_pressure'          , id: 7  },
    'kcal_burned'              : {rel: 'mc_energy_measures',         attr: 'active_energy_burned'              , id: 16 },
    'heart_rate'               : {rel: 'mc_heart_rate_measures',     attr: 'heart_rate'                        , id: 26 },
    'oxygen_saturation'        : {rel: 'mc_blood_measures',          attr: 'blood_oxygen_saturation'           , id: 20 },
    'respiratory_rate'         : {rel: 'mc_lung_measures',           attr: 'respiratory_rate'                  , id: 25 },
    'sleep_duration'           : {rel: 'mc_sleep_measures',          attr: 'sleep_duration'                    , id: 9  },
    'step_count'               : {rel: 'mc_activity_measures',       activity: step_count_activity                      },
    'systolic_blood_pressure'  : {rel: 'mc_blood_pressure_measures', attr: 'systolic_blood_pressure'           , id: 8  }
  };

  var workoutToMCDB = {
    'HKWorkoutActivityTypeAmericanFootball'             : { id: 1    },
    'HKWorkoutActivityTypeArchery'                      : { id: 2    },
    'HKWorkoutActivityTypeAustralianFootball'           : { id: 3    },
    'HKWorkoutActivityTypeBadminton'                    : { id: 4    },
    'HKWorkoutActivityTypeBaseball'                     : { id: 5    },
    'HKWorkoutActivityTypeBasketball'                   : { id: 6    },
    'HKWorkoutActivityTypeBowling'                      : { id: 7    },
    'HKWorkoutActivityTypeBoxing'                       : { id: 8    },
    'HKWorkoutActivityTypeClimbing'                     : { id: 9    },
    'HKWorkoutActivityTypeCricket'                      : { id: 10   },
    'HKWorkoutActivityTypeCrossTraining'                : { id: 11   },
    'HKWorkoutActivityTypeCurling'                      : { id: 12   },
    'HKWorkoutActivityTypeCycling'                      : { id: 13   },
    'HKWorkoutActivityTypeDance'                        : { id: 14   },
    'HKWorkoutActivityTypeDanceInspiredTraining'        : { id: 15   },
    'HKWorkoutActivityTypeElliptical'                   : { id: 16   },
    'HKWorkoutActivityTypeEquestrianSports'             : { id: 17   },
    'HKWorkoutActivityTypeFencing'                      : { id: 18   },
    'HKWorkoutActivityTypeFishing'                      : { id: 19   },
    'HKWorkoutActivityTypeFunctionalStrengthTraining'   : { id: 20   },
    'HKWorkoutActivityTypeGolf'                         : { id: 21   },
    'HKWorkoutActivityTypeGymnastics'                   : { id: 22   },
    'HKWorkoutActivityTypeHandball'                     : { id: 23   },
    'HKWorkoutActivityTypeHiking'                       : { id: 24   },
    'HKWorkoutActivityTypeHockey'                       : { id: 25   },
    'HKWorkoutActivityTypeHunting'                      : { id: 26   },
    'HKWorkoutActivityTypeLacrosse'                     : { id: 27   },
    'HKWorkoutActivityTypeMartialArts'                  : { id: 28   },
    'HKWorkoutActivityTypeMindAndBody'                  : { id: 29   },
    'HKWorkoutActivityTypeMixedMetabolicCardioTraining' : { id: 30   },
    'HKWorkoutActivityTypePaddleSports'                 : { id: 31   },
    'HKWorkoutActivityTypePlay'                         : { id: 32   },
    'HKWorkoutActivityTypePreparationAndRecovery'       : { id: 33   }, // We manually handle P&R with our meal encodings.
    'HKWorkoutActivityTypeRacquetball'                  : { id: 34   },
    'HKWorkoutActivityTypeRowing'                       : { id: 35   },
    'HKWorkoutActivityTypeRugby'                        : { id: 36   },
    'HKWorkoutActivityTypeRunning'                      : { id: 37   },
    'HKWorkoutActivityTypeSailing'                      : { id: 38   },
    'HKWorkoutActivityTypeSkatingSports'                : { id: 39   },
    'HKWorkoutActivityTypeSnowSports'                   : { id: 40   },
    'HKWorkoutActivityTypeSoccer'                       : { id: 41   },
    'HKWorkoutActivityTypeSoftball'                     : { id: 42   },
    'HKWorkoutActivityTypeSquash'                       : { id: 43   },
    'HKWorkoutActivityTypeStairClimbing'                : { id: 44   },
    'HKWorkoutActivityTypeSurfingSports'                : { id: 45   },
    'HKWorkoutActivityTypeSwimming'                     : { id: 46   },
    'HKWorkoutActivityTypeTableTennis'                  : { id: 47   },
    'HKWorkoutActivityTypeTennis'                       : { id: 48   },
    'HKWorkoutActivityTypeTrackAndField'                : { id: 49   },
    'HKWorkoutActivityTypeTraditionalStrengthTraining'  : { id: 50   },
    'HKWorkoutActivityTypeVolleyball'                   : { id: 51   },
    'HKWorkoutActivityTypeWalking'                      : { id: 52   },
    'HKWorkoutActivityTypeWaterFitness'                 : { id: 53   },
    'HKWorkoutActivityTypeWaterPolo'                    : { id: 54   },
    'HKWorkoutActivityTypeWaterSports'                  : { id: 55   },
    'HKWorkoutActivityTypeWrestling'                    : { id: 56   },
    'HKWorkoutActivityTypeYoga'                         : { id: 57   },
    'HKWorkoutActivityTypeOther'                        : { id: 3000 },
  };

  // These identifiers are chosen to ensure no overlap between other workout
  // identifiers (e.g., HKWorkoutActivityTypeOther = 3000).
  var meal_duration_id      = 10;
  var activity_duration_id  = 12;

  // Meal-specific measure ids.
  var breakfast_duration_id   = 40001;
  var lunch_duration_id       = 40002;
  var dinner_duration_id      = 40003;
  var snack_duration_id       = 40004;
  var timed_meal_duration_id  = 40005;

  // Aggregate activity value ids.
  var activity_distance_id = 50001;
  var activity_energy_id   = 50002;

  // Activity-specific measure id offsets.
  var activity_duration_id_offset = 10000;
  var activity_distance_id_offset = 20000;
  var activity_energy_id_offset   = 30000;

  var userid = '';
  var body = {};

  var window_size = '3 months';

  var ts = '';
  var stmt = '';
  var cumulative_stmt = '';

  // Query params: 1: msid, 2: users.id, 3: timestamp, 4: quantity

  if ( TG_OP === "INSERT" ) {
    userid = NEW.body.userid;
    body = NEW.body.body;

    if ( body.effective_time_frame.hasOwnProperty('date_time') ) {
      ts = body.effective_time_frame.date_time;
    } else {
      ts = body.effective_time_frame.time_interval.start_date_time;
    }

    stmt = 'insert into mc_sumcount_by_user as v values ($1, (select max(udbid) from users where id = decode($2, \'base64\')), $3, 1)'
            + ' on conflict(msid, udbid) do update'
            + ' set msum = v.msum + excluded.msum, mcnt = v.mcnt + excluded.mcnt'
            + ' where v.msid = excluded.msid and v.udbid = excluded.udbid';

    cumulative_stmt =
        'with'
      + ' users_matches  as ( select udbid from users where id = decode($2, \'base64\') ),'
      + ' current_window as ( select coalesce(min(W.start_ts), current_timestamp - interval \'' + window_size + '\') as start_ts'
      + '                     from mc_vm_user_windows W where W.msid = $1 and W.udbid = (select max(udbid) from users_matches) ),'
      + ' window_status  as ( select count(*) as include, count(case when ($4::timestamptz) > start_ts then 1 else null::integer end) as slide'
      +                     ' from current_window where ($4::timestamptz) >= (start_ts - interval \'' + window_size + '\') ),'
      + ' new_window as ('
      + '   insert into mc_vm_user_windows as v(msid, udbid, start_ts)'
      + '     select $1 as msid, (select max(udbid) from users_matches) as udbid, ($4::timestamptz) as start_ts from window_status where slide > 0'
      + '   on conflict (msid, udbid) do update'
      + '     set start_ts = greatest(v.start_ts, excluded.start_ts)'
      + '     where v.msid = excluded.msid and v.udbid = excluded.udbid'
      + '   returning start_ts'
      + ' ),'
      + ' by_user_day_upsert as ('
      + '   insert into mc_sumcount_by_user_day as v(msid, udbid, day, msum, mcnt)'
      + '     select $1 as msid, (select max(udbid) from users_matches) as udbid, date_trunc(\'day\', $4::timestamptz) as day, $3 as msum, 1 as mcnt'
      + '     where (select max(include) from window_status) > 0'
      + '   on conflict(msid, udbid, day) do update'
      + '     set msum = v.msum + excluded.msum, mcnt = v.mcnt + excluded.mcnt'
      + '     where v.msid = excluded.msid and v.udbid = excluded.udbid and v.day = excluded.day'
      + '   returning msum, mcnt'
      + ' ),'
      + ' by_user_day_slide as ('
      + '   delete from mc_sumcount_by_user_day'
      + '     where (select max(slide) as slide from window_status) > 0'
      + '     and msid  = $1'
      + '     and udbid = (select max(udbid) from users_matches)'
      + '     and day < (select max(start_ts) - interval \'' + window_size + '\' from new_window)'
      + '   returning day, msum, mcnt'
      + ' ),'
      + ' by_user_upsert as ('
      + '   insert into mc_sumcount_by_user as v(msid, udbid, msum, mcnt)'
      + '     select $1 as msid, (select max(udbid) from users_matches) as udbid,'
      + '            ($3 - coalesce(sum(S.msum), 0)) as msum,'
      + '            (case when (select max(mcnt) as mcnt from by_user_day_upsert) = 1 then 1 else 0 end) - (count(distinct S.day)) as mcnt'
      + '     from by_user_day_slide S'
      + '   on conflict(msid, udbid) do update'
      + '     set msum = v.msum + excluded.msum, mcnt = v.mcnt + excluded.mcnt'
      + '     where v.msid = excluded.msid and v.udbid = excluded.udbid'
      + '   returning msum, mcnt'
      + ' )'
      + ' select max(include) as in_window, max(mcnt) as new_day, max(start_ts) as new_window_start,'
      +        ' max(qdelta) as qdelta, max(cdelta) as cdelta, max(samples_expired) as samples_expired'
      + ' from window_status R, new_window S, by_user_day_upsert T,'
      +      ' (select sum(msum) as qdelta, count(distinct day) as cdelta, sum(mcnt) as samples_expired from by_user_day_slide) U'
      ;
  }
  else if ( TG_OP === "DELETE" ) {
    userid = OLD.body.userid;
    body = OLD.body.body;

    if ( body.effective_time_frame.hasOwnProperty('date_time') ) {
      ts = body.effective_time_frame.date_time;
    } else {
      ts = body.effective_time_frame.time_interval.start_date_time;
    }

    stmt = 'update mc_sumcount_by_user set msum = msum - $3, mcnt = mcnt - 1'
           + ' where msid = $1 and udbid = (select max(udbid) from users where id = decode($2, \'base64\'))';

    cumulative_stmt =
        'with'
      + ' users_matches  as ( select udbid from users where id = decode($2, \'base64\')),'
      + ' current_window as ( select coalesce(min(W.start_ts), current_timestamp) as start_ts'
      + '                     from mc_vm_user_windows W where W.msid = $1 and W.udbid = (select max(udbid) from users_matches) ),'
      + ' window_status  as ( select count(*) as include from current_window'
      +                     ' where ($4::timestamptz) between (start_ts - interval \'' + window_size + '\') and start_ts ),'
      + ' by_user_day_update as ('
      + '   update mc_sumcount_by_user_day as R'
      + '     set msum = msum - $3, mcnt = mcnt - 1'
      + '     where (select max(include) from window_status) > 0'
      + '     and   R.msid  = $1'
      + '     and   R.udbid = (select max(udbid) from users_matches)'
      + '     and   R.day   = date_trunc(\'day\', $4::timestamptz)'
      + '   returning mcnt'
      + ' ),'
      + ' by_user_update as ('
      + '   update mc_sumcount_by_user as R'
      + '     set msum = R.msum - $3, mcnt = (case when (select max(S.mcnt) from by_user_day_update S) = 0 then R.mcnt - 1 else R.mcnt end)'
      + '     where R.msid = $1'
      + '     and   R.udbid = (select max(udbid) from users_matches)'
      + '   returning msum, mcnt'
      + ' )'
      + ' select max(include) as in_window, max(S.mcnt) as deleted_day'
      + ' from window_status R, by_user_day_update S'
      ;
  }

  var on_quantity_type = function(next) {
    var spec = quantityToMCDB[body.quantity_type];

    if ( spec.hasOwnProperty('attrs') ) {
      // Complex quantity value.
      var measure_increments = spec.attrs.map(function (valspec) {
        return [valspec.id, body[valspec.attr].value];
      });

      next(null, measure_increments, null);
    }
    else if ( spec.hasOwnProperty('activity') ) {
      // Handle unnamed quantities that are activities (e.g., steps, distance/walking/running and flights climbed)
      var measure_increments = [];
      var activity_type_id = spec.activity.type_id;
      var activity_duration = spec.activity.duration(body);

      // Duration maintenance
      measure_increments = measure_increments.concat([
        [activity_duration_id, activity_duration],
        [activity_type_id, activity_duration]
      ]);

      // Activity-specific maintenance
      spec.activity.vals.forEach(function (valspec) {
        measure_increments.push([valspec.id, valspec.value(body)]);
      });

      next(null, measure_increments, null);
    }
    else {
      // Simple quantity value.
      if ( spec.hasOwnProperty('count') && spec.count ) {
        next(null, null, [spec.id, userid, body.count]);
      } else {
        next(null, null, [spec.id, userid, body.unit_value.value]);
      }
    }
  };

  // Note: all meals are cumulative in the duration.
  var on_meal = function(next) {
    var meal_type_duration_id = -1;
    if ( Array.isArray(body.metadata) && body.metadata.length > 0 ) {
      if ( body.metadata[0].hasOwnProperty('key') && body.metadata[0].hasOwnProperty('value') ) {
        if ( body.metadata[0].key === 'Meal Type' ) {
          if ( body.metadata[0].value === 'Breakfast' ) {
            meal_type_duration_id = breakfast_duration_id;
          }
          else if ( body.metadata[0].value === 'Lunch' ) {
            meal_type_duration_id = lunch_duration_id;
          }
          else if ( body.metadata[0].value === 'Dinner' ) {
            meal_type_duration_id = dinner_duration_id;
          }
          else if ( body.metadata[0].value === 'Snack' ) {
            meal_type_duration_id = snack_duration_id;
          }
        }
        else if ( body.metadata[0].key === 'Source' && body.metadata[0].value === 'Timer' ) {
          meal_type_duration_id = timed_meal_duration_id;
        }
      }
    }

    if ( meal_type_duration_id > 0 ) {
      var measure_increments = [
        [meal_duration_id, body.duration.value],  // Increment duration aggregate for all meals.
        [meal_type_duration_id, body.duration.value] // Increment duration aggregate for this specific type of meal.
      ];

      next(null, measure_increments, null);
    } else {
      // plv8.elog(ERROR, 'MCVM: Invalid meal type', JSON.stringify(body));
      next('MCVM: Invalid meal type ' + JSON.stringify(body));
    }
  };

  // Note: all activities are cumulative in their values/quantities.
  var on_activity = function(next) {
    var spec = workoutToMCDB[body.activity_name];
    var duration_id = activity_duration_id_offset + spec.id;
    var distance_id = activity_distance_id_offset + spec.id;
    var energy_id   = activity_energy_id_offset + spec.id;
    var measure_increments = [];

    if ( body.hasOwnProperty('distance') ) {
      measure_increments.push([activity_distance_id, body.distance.value], [distance_id, body.distance.value]);
    } else {
      plv8.elog(WARNING, 'Invalid activity distance in', JSON.stringify(body));
    }

    if ( body.hasOwnProperty('duration') ) {
      measure_increments.push([activity_duration_id, body.duration.value], [duration_id, body.duration.value]);
    } else {
      plv8.elog(WARNING, 'Invalid activity duration in', JSON.stringify(body));
    }

    if ( body.hasOwnProperty('kcal_burned') ) {
      measure_increments.push([activity_energy_id, body.kcal_burned.value], [energy_id, body.kcal_burned.value]);
    } else {
      plv8.elog(WARNING, 'Invalid activity kcal in', JSON.stringify(body));
    }

    next(null, measure_increments, null);
  };

  var on_category = function(next) {
    var spec = categoryToMCDB[body.category_type];

    if ( spec.hasOwnProperty("category_value") && body.hasOwnProperty("category_value") ) {
      // Ensure that specs that have a category_value filter only process data that matches this value.
      if ( body.category_value === spec.category_value ) {
        next(null, null, [spec.id, userid, spec.value_extractor(body)]);
      }
    }
    else {
      // Process all data tf there is no category_value specified in the spec.
      next(null, null, [spec.id, userid, spec.value_extractor(body)]);
    }
  };

  var on_named = function(mc_name, next) {
    var spec = granolaToMCDB[mc_name];

    if ( spec.hasOwnProperty('attrs') ) {
      // Complex quantity value.
      var measure_increments = spec.attrs.map(function (valspec) {
        return [valspec.id, body[valspec.attr].value];
      });

      next(null, measure_increments, null);
    }
    else if ( spec.hasOwnProperty('activity') ) {
      // Handle named quantities that are activities.
      // Note: These do not contribute to aggregate distance/kcal_burned measures.
      var measure_increments = [];
      var activity_type_id = spec.activity.type_id;
      var activity_duration = spec.activity.duration(body);

      // Duration maintenance
      measure_increments = measure_increments.concat([
        [activity_duration_id, activity_duration],
        [activity_type_id, activity_duration]
      ]);

      // Activity-specific maintenance
      spec.activity.vals.forEach(function (valspec) {
        measure_increments.push([valspec.id, valspec.value(body)]);
      });

      next(null, measure_increments, null);
    }
    else {
      if ( spec.hasOwnProperty('count') && spec.count ) {
        next(null, null, [spec.id, userid, body[mc_name]]);
      } else {
        next(null, null, [spec.id, userid, body[mc_name].value]);
      }
    }
  };

  var cumulative_measure = function(spec_table, key, next) {
    var spec = spec_table[key];
    var mc_name = '';

    if ( spec.hasOwnProperty('attrs') ) {
      // Complex quantity value.
      // TODO: for now this is only blood pressure.
      return next(null, false);
    }
    else if ( spec.hasOwnProperty('activity') ) {
      mc_name = 'activity_value';
    }
    else if ( spec.hasOwnProperty('attr') ) {
      // Simple quantity value.
      mc_name = spec.attr;
    }

    if ( !strIsEmpty(mc_name) ) {
      if ( mc_attrs_acquisition_class.hasOwnProperty(mc_name) ) {
        next(null, mc_attrs_acquisition_class[mc_name].cumulative);
      } else {
        next('MCVM: No acquisition class found HealthKit/MCDB measure ' + mc_name);
      }
    } else {
      next('MCVM: Unknown HealthKit quantity type ' + quantity_type);
    }
  };

  var cumulative_quantity = function(quantity_type, next) {
    cumulative_measure(quantityToMCDB, quantity_type, next);
  };

  var cumulative_named_quantity = function(mc_name, next) {
    cumulative_measure(granolaToMCDB, mc_name, next);
  };

  var dispatch_query = function(cumulative, query, params_array, params) {
    if ( params_array ) {
      params_array.forEach(function(kv) {
        var qparams = cumulative ? [kv[0], userid, kv[1], ts] : [kv[0], userid, kv[1]];
        plv8.execute(query, qparams);
      });
    } else {
      var qparams = cumulative ? [params[0], params[1], params[2], ts] : params;
      plv8.execute(query, qparams);
    }
  };

  if ( !strIsEmpty(userid) && !objIsEmpty(body) && !strIsEmpty(stmt) ) {
    if ( body.hasOwnProperty('quantity_type') ) {
      // Handle HealthKit measure.
      if ( quantityToMCDB.hasOwnProperty(body.quantity_type) ) {
        on_quantity_type(function(err, params_array, params) {
          if ( err ) { plv8.elog(ERROR, err); } else {
            cumulative_quantity(body.quantity_type, function(err, is_cumulative) {
              if ( err ) { plv8.elog(ERROR, err); } else {
                var query = is_cumulative ? cumulative_stmt : stmt;
                dispatch_query(is_cumulative, query, params_array, params);
              }
            });
          }
        });
      } else {
        plv8.elog(ERROR, 'MCVM: Unsupported quantity type: ', body.quantity_type);
      }
    }
    else if ( body.hasOwnProperty('activity_name') ) {
      // Handle meals encoded as PreparationAndRecovery workouts.
      var isMeal = body.activity_name === 'HKWorkoutActivityTypePreparationAndRecovery'
                    && body.metadata.some(function (kv) { return (kv[1] === 'Meal Type' || kv[1] === 'Timer'); });

      if ( isMeal ) {
        on_meal(function(err, params_array, params) {
          if ( err ) { plv8.elog(ERROR, err); } else {
            // TODO: all meals are cumulative in the duration, so we always dispatch the cumulative stmt.
            dispatch_query(true, cumulative_stmt, params_array, params);
          }
        });
      } else {
        // Handle HealthKit workout.
        on_activity(function(err, params_array, params) {
          if ( err ) { plv8.elog(ERROR, err); } else {
            // TODO: all activities are cumulative in their values/quantities, so we always dispatch the cumulative stmt.
            dispatch_query(true, cumulative_stmt, params_array, params);
          }
        });
      }
    }
    else if ( body.hasOwnProperty('category_type') ) {
      on_category(function(err, params_array, params) {
        if ( err ) { plv8.elog(ERROR, err); } else {
          // Note: for now, no categories are cumulative, so we always dispatch the standard stmt.
          dispatch_query(false, stmt, params_array, params);
        }
      });
    }
    else {
      // Handle Granola named measure.
      var found = false;
      var test_attrs = Object.keys(granolaToMCDB);
      for (var i = 0; i < test_attrs.length; i++) {
        if ( body.hasOwnProperty(test_attrs[i]) ) {
          on_named(test_attrs[i], function(err, params_array, params) {
            if ( err ) { plv8.elog(ERROR, err); } else {
              cumulative_named_quantity(test_attrs[i], function(err, is_cumulative) {
                if ( err ) { plv8.elog(ERROR, err); } else {
                  var query = is_cumulative ? cumulative_stmt : stmt;
                  dispatch_query(is_cumulative, query, params_array, params);
                }
              });
            }
          });
          found = true;
          break;
        }
      }

      if ( !found ) {
        plv8.elog(ERROR, 'MCVM: Unsupported named measure: ', JSON.stringify(body));
      }
    }
  }

  // Returns are ignored for after-triggers;
  return null;
$$
language plv8;

drop trigger if exists on_insert_mc_granola_measures on mc_granola_measures;
drop trigger if exists on_delete_mc_granola_measures on mc_granola_measures;

create trigger on_insert_mc_granola_measures
after insert on mc_granola_measures
for each row execute procedure mc_granola_measures_sumcount_vm_fn();

create trigger on_delete_mc_granola_measures
after delete on mc_granola_measures
for each row execute procedure mc_granola_measures_sumcount_vm_fn();

create or replace function mc_json_measures_sumcount_vm_fn() returns trigger as
$$
  // Utilities
  var objIsEmpty = function(o) { return Object.keys(o).length == 0; };
  var strIsEmpty = function(s) { return (!s || s === ''); };

  var hasAllProperties = function(v, arr) {
    return arr.every(function (k) { return v.hasOwnProperty(k); });
  };

  // meal_duration, activity_duration and activity_value are explicitly handled
  // so we add them to the exclude list for the generic attributes updates here.
  var excludes = {
    'meal_duration'                        : 10 ,
    'food_type'                            : 11 ,
    'activity_duration'                    : 12 ,
    'activity_type'                        : 13 ,
    'activity_value'                       : 14 ,
  };

  var mc_schema = {
    'body_weight'                          : 0  ,
    'body_height'                          : 1  ,
    'body_mass_index'                      : 2  ,
    'body_fat_percentage'                  : 3  ,
    'body_temperature'                     : 4  ,
    'basal_body_temperature'               : 5  ,
    'lean_body_mass'                       : 6  ,
    'systolic_blood_pressure'              : 7  ,
    'diastolic_blood_pressure'             : 8  ,
    'sleep_duration'                       : 9  ,
    'meal_duration'                        : 10 ,
    'food_type'                            : 11 ,
    'activity_duration'                    : 12 ,
    'activity_type'                        : 13 ,
    'activity_value'                       : 14 ,
    'uv_exposure'                          : 15 ,
    'active_energy_burned'                 : 16 ,
    'basal_energy_burned'                  : 17 ,
    'blood_alcohol_content'                : 18 ,
    'blood_glucose'                        : 19 ,
    'blood_oxygen_saturation'              : 20 ,
    'forced_expiratory_volume_one_second'  : 21 ,
    'forced_vital_capacity'                : 22 ,
    'peak_expiratory_flow'                 : 23 ,
    'inhaler_usage'                        : 24 ,
    'respiratory_rate'                     : 25 ,
    'heart_rate'                           : 26 ,
    'dietary_carbohydrates'                : 27 ,
    'dietary_energy_consumed'              : 28 ,
    'dietary_fat_total'                    : 29 ,
    'dietary_protein'                      : 30 ,
    'dietary_caffeine'                     : 31 ,
    'dietary_cholesterol'                  : 32 ,
    'dietary_fat_monounsaturated'          : 33 ,
    'dietary_fat_polyunsaturated'          : 34 ,
    'dietary_fat_saturated'                : 35 ,
    'dietary_fiber'                        : 36 ,
    'dietary_sugar'                        : 37 ,
    'dietary_calcium'                      : 38 ,
    'dietary_chloride'                     : 39 ,
    'dietary_chromium'                     : 40 ,
    'dietary_copper'                       : 41 ,
    'dietary_iodine'                       : 42 ,
    'dietary_iron'                         : 43 ,
    'dietary_magnesium'                    : 44 ,
    'dietary_manganese'                    : 45 ,
    'dietary_molybdenum'                   : 46 ,
    'dietary_niacin'                       : 47 ,
    'dietary_phosphorus'                   : 48 ,
    'dietary_potassium'                    : 49 ,
    'dietary_selenium'                     : 50 ,
    'dietary_sodium'                       : 51 ,
    'dietary_zinc'                         : 52 ,
    'dietary_biotin'                       : 53 ,
    'dietary_folate'                       : 54 ,
    'dietary_pantothenic_acid'             : 55 ,
    'dietary_riboflavin'                   : 56 ,
    'dietary_thiamin'                      : 57 ,
    'dietary_vitamina'                     : 58 ,
    'dietary_vitaminb12'                   : 59 ,
    'dietary_vitaminb6'                    : 60 ,
    'dietary_vitaminc'                     : 61 ,
    'dietary_vitamind'                     : 62 ,
    'dietary_vitamine'                     : 63 ,
    'dietary_vitamink'                     : 64 ,
    'dietary_alcohol'                      : 65 ,
    'dietary_water'                        : 66 ,
    'apple_stand_hour'                     : 67 ,
    'electrodermal_activity'               : 68 ,
    'nike_fuel'                            : 69 ,
    'number_of_times_fallen'               : 70 ,
    'peripheral_perfusion_index'           : 71
  };

  var workoutToMCDB = {
    1    : 'HKWorkoutActivityTypeAmericanFootball'             ,
    2    : 'HKWorkoutActivityTypeArchery'                      ,
    3    : 'HKWorkoutActivityTypeAustralianFootball'           ,
    4    : 'HKWorkoutActivityTypeBadminton'                    ,
    5    : 'HKWorkoutActivityTypeBaseball'                     ,
    6    : 'HKWorkoutActivityTypeBasketball'                   ,
    7    : 'HKWorkoutActivityTypeBowling'                      ,
    8    : 'HKWorkoutActivityTypeBoxing'                       ,
    9    : 'HKWorkoutActivityTypeClimbing'                     ,
    10   : 'HKWorkoutActivityTypeCricket'                      ,
    11   : 'HKWorkoutActivityTypeCrossTraining'                ,
    12   : 'HKWorkoutActivityTypeCurling'                      ,
    13   : 'HKWorkoutActivityTypeCycling'                      ,
    14   : 'HKWorkoutActivityTypeDance'                        ,
    15   : 'HKWorkoutActivityTypeDanceInspiredTraining'        ,
    16   : 'HKWorkoutActivityTypeElliptical'                   ,
    17   : 'HKWorkoutActivityTypeEquestrianSports'             ,
    18   : 'HKWorkoutActivityTypeFencing'                      ,
    19   : 'HKWorkoutActivityTypeFishing'                      ,
    20   : 'HKWorkoutActivityTypeFunctionalStrengthTraining'   ,
    21   : 'HKWorkoutActivityTypeGolf'                         ,
    22   : 'HKWorkoutActivityTypeGymnastics'                   ,
    23   : 'HKWorkoutActivityTypeHandball'                     ,
    24   : 'HKWorkoutActivityTypeHiking'                       ,
    25   : 'HKWorkoutActivityTypeHockey'                       ,
    26   : 'HKWorkoutActivityTypeHunting'                      ,
    27   : 'HKWorkoutActivityTypeLacrosse'                     ,
    28   : 'HKWorkoutActivityTypeMartialArts'                  ,
    29   : 'HKWorkoutActivityTypeMindAndBody'                  ,
    30   : 'HKWorkoutActivityTypeMixedMetabolicCardioTraining' ,
    31   : 'HKWorkoutActivityTypePaddleSports'                 ,
    32   : 'HKWorkoutActivityTypePlay'                         ,
    33   : 'HKWorkoutActivityTypePreparationAndRecovery'       , // We manually handle P&R with our meal encodings.
    34   : 'HKWorkoutActivityTypeRacquetball'                  ,
    35   : 'HKWorkoutActivityTypeRowing'                       ,
    36   : 'HKWorkoutActivityTypeRugby'                        ,
    37   : 'HKWorkoutActivityTypeRunning'                      ,
    38   : 'HKWorkoutActivityTypeSailing'                      ,
    39   : 'HKWorkoutActivityTypeSkatingSports'                ,
    40   : 'HKWorkoutActivityTypeSnowSports'                   ,
    41   : 'HKWorkoutActivityTypeSoccer'                       ,
    42   : 'HKWorkoutActivityTypeSoftball'                     ,
    43   : 'HKWorkoutActivityTypeSquash'                       ,
    44   : 'HKWorkoutActivityTypeStairClimbing'                ,
    45   : 'HKWorkoutActivityTypeSurfingSports'                ,
    46   : 'HKWorkoutActivityTypeSwimming'                     ,
    47   : 'HKWorkoutActivityTypeTableTennis'                  ,
    48   : 'HKWorkoutActivityTypeTennis'                       ,
    49   : 'HKWorkoutActivityTypeTrackAndField'                ,
    50   : 'HKWorkoutActivityTypeTraditionalStrengthTraining'  ,
    51   : 'HKWorkoutActivityTypeVolleyball'                   ,
    52   : 'HKWorkoutActivityTypeWalking'                      ,
    53   : 'HKWorkoutActivityTypeWaterFitness'                 ,
    54   : 'HKWorkoutActivityTypeWaterPolo'                    ,
    55   : 'HKWorkoutActivityTypeWaterSports'                  ,
    56   : 'HKWorkoutActivityTypeWrestling'                    ,
    57   : 'HKWorkoutActivityTypeYoga'                         ,
    3000 : 'HKWorkoutActivityTypeOther'                        ,
  };

  var mc_attrs_acquisition_class = {
    'body_weight'                         : {cumulative: false },
    'body_height'                         : {cumulative: false },
    'body_mass_index'                     : {cumulative: false },
    'body_fat_percentage'                 : {cumulative: false },
    'body_temperature'                    : {cumulative: false },
    'basal_body_temperature'              : {cumulative: false },
    'lean_body_mass'                      : {cumulative: false },
    'systolic_blood_pressure'             : {cumulative: false },
    'diastolic_blood_pressure'            : {cumulative: false },
    'sleep_duration'                      : {cumulative: false },
    'meal_duration'                       : {cumulative: true  },  // With cumulative meal durations, we sum up the durations per day (optionally by type)
    'food_type'                           : {cumulative: false },
    'activity_duration'                   : {cumulative: true  },  // With cumulative activity durations, we sum up the durations per day (optionally by type)
    'activity_type'                       : {cumulative: false },
    'activity_value'                      : {cumulative: true  },  // With cumulative activity values, we sum up the activity quantities per day (optionally by type).
    'uv_exposure'                         : {cumulative: false },
    'active_energy_burned'                : {cumulative: true  },
    'basal_energy_burned'                 : {cumulative: true  },
    'blood_alcohol_content'               : {cumulative: false },
    'blood_glucose'                       : {cumulative: false },
    'blood_oxygen_saturation'             : {cumulative: false },
    'forced_expiratory_volume_one_second' : {cumulative: false },
    'forced_vital_capacity'               : {cumulative: false },
    'peak_expiratory_flow'                : {cumulative: false },
    'inhaler_usage'                       : {cumulative: true  },
    'respiratory_rate'                    : {cumulative: false },
    'heart_rate'                          : {cumulative: false },
    'dietary_carbohydrates'               : {cumulative: true  },
    'dietary_energy_consumed'             : {cumulative: true  },
    'dietary_fat_total'                   : {cumulative: true  },
    'dietary_protein'                     : {cumulative: true  },
    'dietary_caffeine'                    : {cumulative: true  },
    'dietary_cholesterol'                 : {cumulative: true  },
    'dietary_fat_monounsaturated'         : {cumulative: true  },
    'dietary_fat_polyunsaturated'         : {cumulative: true  },
    'dietary_fat_saturated'               : {cumulative: true  },
    'dietary_fiber'                       : {cumulative: true  },
    'dietary_sugar'                       : {cumulative: true  },
    'dietary_calcium'                     : {cumulative: true  },
    'dietary_chloride'                    : {cumulative: true  },
    'dietary_chromium'                    : {cumulative: true  },
    'dietary_copper'                      : {cumulative: true  },
    'dietary_iodine'                      : {cumulative: true  },
    'dietary_iron'                        : {cumulative: true  },
    'dietary_magnesium'                   : {cumulative: true  },
    'dietary_manganese'                   : {cumulative: true  },
    'dietary_molybdenum'                  : {cumulative: true  },
    'dietary_niacin'                      : {cumulative: true  },
    'dietary_phosphorus'                  : {cumulative: true  },
    'dietary_potassium'                   : {cumulative: true  },
    'dietary_selenium'                    : {cumulative: true  },
    'dietary_sodium'                      : {cumulative: true  },
    'dietary_zinc'                        : {cumulative: true  },
    'dietary_biotin'                      : {cumulative: true  },
    'dietary_folate'                      : {cumulative: true  },
    'dietary_pantothenic_acid'            : {cumulative: true  },
    'dietary_riboflavin'                  : {cumulative: true  },
    'dietary_thiamin'                     : {cumulative: true  },
    'dietary_vitamina'                    : {cumulative: true  },
    'dietary_vitaminb12'                  : {cumulative: true  },
    'dietary_vitaminb6'                   : {cumulative: true  },
    'dietary_vitaminc'                    : {cumulative: true  },
    'dietary_vitamind'                    : {cumulative: true  },
    'dietary_vitamine'                    : {cumulative: true  },
    'dietary_vitamink'                    : {cumulative: true  },
    'dietary_alcohol'                     : {cumulative: true  },
    'dietary_water'                       : {cumulative: true  },
    'apple_stand_hour'                    : {cumulative: false },
    'electrodermal_activity'              : {cumulative: false },
    'nike_fuel'                           : {cumulative: true  },
    'number_of_times_fallen'              : {cumulative: true  },
    'peripheral_perfusion_index'          : {cumulative: false },
  };


  // These identifiers are chosen to ensure no overlap between other workout
  // identifiers (e.g., HKWorkoutActivityTypeOther = 3000).
  var meal_duration_id      = 10;
  var activity_duration_id  = 12;

  // Meal-specific measure id offsets.
  var breakfast_duration_id   = 40001;
  var lunch_duration_id       = 40002;
  var dinner_duration_id      = 40003;
  var snack_duration_id       = 40004;
  var timed_meal_duration_id  = 40005;

  // Aggregate activity value ids.
  var activity_distance_id = 50001;
  var activity_energy_id   = 50002;

  // Activity-specific measure id offsets.
  var activity_duration_id_offset = 10000;
  var activity_distance_id_offset = 20000;
  var activity_energy_id_offset   = 30000;

  var dwr_distance_offset = 20000;

  var userid = '';
  var body = {};

  var window_size = '3 months';

  var ts = '';
  var stmt = '';
  var cumulative_stmt = '';

  if ( TG_OP === "INSERT" ) {
    userid = NEW.userid;
    body = NEW.body;
    ts = NEW.body.ts;

    stmt = 'insert into mc_sumcount_by_user as v values ($1, (select max(udbid) from users where id = decode($2, \'base64\')), $3, 1)'
            + ' on conflict(msid, udbid) do update'
            + ' set msum = v.msum + excluded.msum, mcnt = v.mcnt + excluded.mcnt'
            + ' where v.msid = excluded.msid and v.udbid = excluded.udbid';

    cumulative_stmt =
        'with'
      + ' users_matches  as ( select udbid from users where id = decode($2, \'base64\')),'
      + ' current_window as ( select coalesce(min(W.start_ts), current_timestamp - interval \'' + window_size + '\') as start_ts'
      + '                     from mc_vm_user_windows W where W.msid = $1 and W.udbid = (select max(udbid) from users_matches) ),'
      + ' window_status  as ( select count(*) as include, count(case when ($4::timestamptz) > start_ts then 1 else null end) as slide'
      +                     ' from current_window where ($4::timestamptz) >= (start_ts - interval \'' + window_size + '\') ),'
      + ' new_window as ('
      + '   insert into mc_vm_user_windows as v(msid, udbid, start_ts)'
      + '     select $1 as msid, (select max(udbid) from users_matches) as udbid, ($4::timestamptz) as start_ts from window_status where slide > 0'
      + '   on conflict (msid, udbid) do update'
      + '     set start_ts = greatest(v.start_ts, excluded.start_ts)'
      + '     where v.msid = excluded.msid and v.udbid = excluded.udbid'
      + '   returning start_ts'
      + ' ),'
      + ' by_user_day_upsert as ('
      + '   insert into mc_sumcount_by_user_day as v(msid, udbid, day, msum, mcnt)'
      + '     select $1 as msid, (select max(udbid) from users_matches) as udbid, date_trunc(\'day\', $4::timestamptz) as day, $3 as msum, 1 as mcnt'
      + '     where (select max(include) from window_status) > 0'
      + '   on conflict(msid, udbid, day) do update'
      + '     set msum = v.msum + excluded.msum, mcnt = v.mcnt + excluded.mcnt'
      + '     where v.msid = excluded.msid and v.udbid = excluded.udbid and v.day = excluded.day'
      + '   returning msum, mcnt'
      + ' ),'
      + ' by_user_day_slide as ('
      + '   delete from mc_sumcount_by_user_day'
      + '     where (select max(slide) as slide from window_status) > 0'
      + '     and msid  = $1'
      + '     and udbid = (select max(udbid) from users_matches)'
      + '     and day < (select max(start_ts) - interval \'' + window_size + '\' from new_window)'
      + '   returning day, msum, mcnt'
      + ' ),'
      + ' by_user_upsert as ('
      + '   insert into mc_sumcount_by_user as v(msid, udbid, msum, mcnt)'
      + '     select $1 as msid, (select max(udbid) from users_matches) as udbid,'
      + '            ($3 - coalesce(sum(S.msum), 0)) as msum,'
      + '            (case when (select max(mcnt) as mcnt from by_user_day_upsert) = 1 then 1 else 0 end) - (count(distinct S.day)) as mcnt'
      + '     from by_user_day_slide S'
      + '   on conflict(msid, udbid) do update'
      + '     set msum = v.msum + excluded.msum, mcnt = v.mcnt + excluded.mcnt'
      + '     where v.msid = excluded.msid and v.udbid = excluded.udbid'
      + '   returning msum, mcnt'
      + ' )'
      + ' select max(include) as in_window, max(mcnt) as new_day, max(start_ts) as new_window_start,'
      +        ' max(qdelta) as qdelta, max(cdelta) as cdelta, max(samples_expired) as samples_expired'
      + ' from window_status R, new_window S, by_user_day_upsert T,'
      +      ' (select sum(msum) as qdelta, count(distinct day) as cdelta, sum(mcnt) as samples_expired from by_user_day_slide) U'
      ;
  }
  else if ( TG_OP === "DELETE" ) {
    userid = OLD.userid;
    body = OLD.body;
    ts = OLD.body.ts;

    stmt = 'update mc_sumcount_by_user set msum = msum - $3, mcnt = mcnt - 1'
            + ' where msid = $1 and udbid = (select max(udbid) from users where id = decode($2, \'base64\'))';

    cumulative_stmt =
        'with'
      + ' users_matches  as ( select udbid from users where id = decode($2, \'base64\')),'
      + ' current_window as ( select coalesce(min(W.start_ts), current_timestamp) as start_ts'
      + '                     from mc_vm_user_windows W where W.msid = $1 and W.udbid = (select max(udbid) from users_matches) ),'
      + ' window_status  as ( select count(*) as include from current_window'
      +                     ' where ($4::timestamptz) between (start_ts - interval \'' + window_size + '\') and start_ts ),'
      + ' by_user_day_update as ('
      + '   update mc_sumcount_by_user_day as R'
      + '     set msum = msum - $3, mcnt = mcnt - 1'
      + '     where (select max(include) from window_status) > 0'
      + '     and   R.msid  = $1'
      + '     and   R.udbid = (select max(udbid) from users_matches)'
      + '     and   R.day   = date_trunc(\'day\', $4::timestamptz)'
      + '   returning mcnt'
      + ' ),'
      + ' by_user_update as ('
      + '   update mc_sumcount_by_user as R'
      + '     set msum = R.msum - $3, mcnt = (case when (select max(S.mcnt) from by_user_day_update S) = 0 then R.mcnt - 1 else R.mcnt end)'
      + '     where R.msid = $1'
      + '     and   R.udbid = (select max(udbid) from users_matches)'
      + '   returning msum, mcnt'
      + ' )'
      + ' select max(include) as in_window, max(S.mcnt) as deleted_day'
      + ' from window_status R, by_user_day_update S'
      ;
  }

  var on_meal = function(next) {
    var meal_type_duration_id = -1;
    if ( body.food_type.hasOwnProperty('key') && body.food_type.hasOwnProperty('value') ) {
      if ( body.food_type.key === 'Meal Type' ) {
        if ( body.food_type.value === 'Breakfast' ) {
            meal_type_duration_id = breakfast_duration_id;
        }
        else if ( body.food_type.value === 'Lunch' ) {
            meal_type_duration_id = lunch_duration_id;
        }
        else if ( body.food_type.value === 'Dinner' ) {
            meal_type_duration_id = dinner_duration_id;
        }
        else if ( body.food_type.value === 'Snack' ) {
            meal_type_duration_id = snack_duration_id;
        }
      }
      else if ( body.food_type.key === 'Source' && body.food_type.value === 'Timer' ) {
        meal_type_duration_id = timed_meal_duration_id;
      }
    }

    if ( meal_type_duration_id > 0 ) {
      var measure_increments = [
        [meal_duration_id, body.meal_duration],  // Increment duration aggregate for all meals.
        [meal_type_duration_id, body.meal_duration] // Increment duration aggregate for this specific type of meal.
      ];

      next(null, measure_increments, null);
    } else {
      // plv8.elog(ERROR, 'MCVM: Invalid meal type', JSON.stringify(body));
      next('MCVM: Invalid meal type' + JSON.stringify(body));
    }
  };

  var on_activity = function(next) {
    var measure_increments = [
      [activity_duration_id, body.activity_duration]
    ];

    var activity_id = parseInt(body.activity_type);

    if ( hasAllProperties(body.activity_value, ['distance', 'kcal_burned']) ) {
      // All workouts.
      var duration_id = activity_duration_id_offset + activity_id;
      var distance_id = activity_distance_id_offset + activity_id;
      var energy_id   = activity_energy_id_offset   + activity_id;
      measure_increments = measure_increments.concat([
        [activity_distance_id, JSON.parse(body.activity_value.distance).value],
        [activity_energy_id, JSON.parse(body.activity_value.kcal_burned).value],
        [duration_id, body.activity_duration],
        [distance_id, JSON.parse(body.activity_value.distance).value],
        [energy_id, JSON.parse(body.activity_value.kcal_burned).value]
      ]);
    }
    else if ( hasAllProperties(body.activity_value, ['distance']) ) {
      // Distance/walking/running encoded as an activity.
      var duration_id = activity_duration_id_offset + activity_id;
      var distance_id = activity_distance_id_offset + activity_id;
      measure_increments = measure_increments.concat([
        [activity_distance_id, JSON.parse(body.activity_value.distance).value],
        [duration_id, body.activity_duration],
        [distance_id, JSON.parse(body.activity_value.distance).value]
      ]);

    } else if ( hasAllProperties(body.activity_value, ['step_count']) ) {
      // Step counts encoded as an activity.
      var duration_id = activity_duration_id_offset + activity_id;
      var count_id    = activity_distance_id_offset + activity_id;
      measure_increments = measure_increments.concat([
        [duration_id, body.activity_duration],
        [count_id, body.activity_value.step_count]
      ]);
    } else if ( hasAllProperties(body.activity_value, ['flights']) ) {
      // Flights climbed encoded as an activity.
      var duration_id = activity_duration_id_offset + activity_id;
      var flights_id  = activity_distance_id_offset + activity_id;
      measure_increments = measure_increments.concat([
        [duration_id, body.activity_duration],
        [flights_id, body.activity_value.flights]
      ]);
    }

    next(null, measure_increments, null);
  };

  var cumulative_measure = function(mc_name, next) {
    if ( mc_attrs_acquisition_class.hasOwnProperty(mc_name) ) {
      next(null, mc_attrs_acquisition_class[mc_name].cumulative);
    } else {
      next('MCVM: No acquisition class found MCDB measure ' + mc_name);
    }
  };

  var dispatch_query = function(cumulative, query, params_array, params) {
    if ( params_array ) {
      params_array.forEach(function(kv) {
        var qparams = cumulative ? [kv[0], userid, kv[1], ts] : [kv[0], userid, kv[1]];
        plv8.execute(query, qparams);
      });
    } else {
      var qparams = cumulative ? [params[0], params[1], params[2], ts] : params;
      plv8.execute(query, qparams);
    }
  };

  if ( !strIsEmpty(userid) && !objIsEmpty(body) && !strIsEmpty(stmt) ) {
    // Handle any meals.
    if ( hasAllProperties(body, ['meal_duration', 'food_type']) ) {
      on_meal(function(err, params_array, params) {
        if ( err ) { plv8.elog(ERROR, err); } else {
          dispatch_query(true, cumulative_stmt,  params_array, params);
        }
      });
    }

    // Handle any activities.
    if ( hasAllProperties(body, ['activity_duration', 'activity_type', 'activity_value']) ) {
      // Handle HealthKit workout.
      on_activity(function(err, params_array, params) {
        if ( err ) { plv8.elog(ERROR, err); } else {
          dispatch_query(true, cumulative_stmt,  params_array, params);
        }
      });
    }

    // Handle remaining measures.
    Object.keys(body).forEach(function(key) {
      if ( !excludes.hasOwnProperty(key) ) {
        if ( mc_schema.hasOwnProperty(key) ) {
          cumulative_measure(key, function(err, is_cumulative) {
            var query = is_cumulative ? cumulative_stmt : stmt;
            var qparams = is_cumulative ? [mc_schema[key], userid, body[key], ts] : [mc_schema[key], userid, body[key]];
            plv8.execute(query, qparams);
          });
        } else if ( key !== 'ts' ) {
          plv8.elog(ERROR, 'MCVM: Invalid MC-JSON attribute: ', key);
        }
      }
    });
  }

  // Returns are ignored for after-triggers;
  return null;
$$
language plv8;

drop trigger if exists on_insert_mc_json_measures on mc_json_measures;
drop trigger if exists on_delete_mc_json_measures on mc_json_measures;

create trigger on_insert_mc_json_measures
after insert on mc_json_measures
for each row execute procedure mc_json_measures_sumcount_vm_fn();

create trigger on_delete_mc_json_measures
after delete on mc_json_measures
for each row execute procedure mc_json_measures_sumcount_vm_fn();
