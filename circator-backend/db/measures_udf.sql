--
-- A generalized population aggregation query.
--
-- This aggregates across four different datasets:
-- a. NHANES
-- b. MC-granola
-- c. MC-json
-- d. MC-relational
--
create or replace function mc_generalized_population_query(parameters jsonb, view_initializer boolean)
  returns setof jsonb as
$$
  //////////////////////////
  // Utility functions.

  // Copied from underscore.js definition.
  var property = function(key) {
    return function(obj) {
      return obj == null ? void 0 : obj[key];
    };
  };

  var isArray = function(obj) { return Array.isArray(obj); }

  var isString = function(obj) {
    return toString.call(obj) === '[object String]';
  };

  var isArguments = function(obj) {
    return toString.call(obj) === '[object Arguments]';
  };

  // Builds an object from an array of key-value pairs,
  // where pairs are encoded as 2-element arrays.
  var objectFromKV = function(arr) {
    return arr.reduce(function (acc, kv) {
      acc[kv[0]] = kv[1];
      return acc;
    }, {});
  };

  // Extends the first object from the second, and returns the first object.
  var extendObject = function(a, b) {
    Object.keys(b).forEach(function(kb) {
      a[kb] = b[kb];
    });
    return a;
  };

  // Returns an object projected to the given keys.
  var pickObject = function(obj, keys) {
    var result = {};
    keys.forEach(function(k) {
      if ( obj.hasOwnProperty(k) ) { result[k] = obj[k]; }
    });
    return result;
  };

  // Helper for collection methods to determine whether a collection
  // should be iterated as an array or as an object.
  // Related: http://people.mozilla.org/~jorendorff/es6-draft.html#sec-tolength
  // Avoids a very nasty iOS 8 JIT bug on ARM-64. #2094
  var MAX_ARRAY_INDEX = Math.pow(2, 53) - 1;
  var getLength = property('length');
  var isArrayLike = function(collection) {
    var length = getLength(collection);
    return typeof length == 'number' && length >= 0 && length <= MAX_ARRAY_INDEX;
  };

  var partitionArray = function(arr, predicate) {
    var pass = [], fail = [];
    arr.forEach(function(elem) {
      (predicate(elem) ? pass : fail).push(elem);
    });
    return [pass, fail];
  };

  var uniqueArray = function(arr) {
    return arr.filter(function (v, i, self) { return self.indexOf(v) === i; });
  };

  // Taken from https://github.com/jashkenas/underscore/blob/master/underscore.js
  var flattenArray = function(input, shallow, strict, output) {
    output = output || [];
    var idx = output.length;
    for (var i = 0, length = getLength(input); i < length; i++) {
      var value = input[i];
      if (isArrayLike(value) && (isArray(value) || isArguments(value))) {
        // Flatten current level of array or arguments object.
        if (shallow) {
          var j = 0, len = value.length;
          while (j < len) output[idx++] = value[j++];
        } else {
          flattenArray(value, shallow, strict, output);
          idx = output.length;
        }
      } else if (!strict) {
        output[idx++] = value;
      }
    }
    return output;
  };

  var arrIsEmpty = function(a) { return a.length === 0; };
  var objIsEmpty = function(o) { return Object.keys(o).length === 0; };
  var strIsEmpty = function(s) { return (!s || s === ''); };

  var anyArray = function(arr, cb) {
    return arr.reduce(function(acc, i) { return ( acc || cb(i) ); }, false);
  };

  var allArray = function(arr, cb) {
    return arr.reduce(function(acc, i) { return ( acc && cb(i) ); }, true);
  };

  //////////////////////////
  // Parameter checking.
  var required = ['aggregate', 'tstart', 'tend', 'use_nhanes', 'use_granola', 'use_mc_json', 'use_mc'];
  var present = required.map(function(p) { return parameters.hasOwnProperty(p); });

  if ( !present.every(function (i) { return i; }) ) {
    plv8.elog(ERROR, 'Missing required parameters: ',
        present[0] ? '' : required[0],
        present[1] ? '' : required[1],
        present[2] ? '' : required[2],
        present[3] ? '' : required[3],
        present[4] ? '' : required[4],
        present[5] ? '' : required[5],
        present[6] ? '' : required[6]);
    return null;
  }

  var aggregate   = parameters.aggregate;
  var tstart      = parseInt(parameters.tstart) || 0;
  var tend        = parseInt(parameters.tend)   || ( (new Date()).getTime() / 1000 );

  var with_explicit_population = parameters.hasOwnProperty('population');

  if ( view_initializer && with_explicit_population ) {
    plv8.elog(ERROR, 'Cannot perform view initialization with an explicit population');
    return null;
  }

  var use_nhanes  = parameters.use_nhanes && !with_explicit_population;
  var use_granola = parameters.use_granola;
  var use_mc_json = parameters.use_mc_json;
  var use_mc      = parameters.use_mc;

  var with_parameters = use_granola || use_mc_json || use_mc;

  var valid_aggregates = ['avg', 'min', 'max'];
  if ( !( anyArray(valid_aggregates, function (a) { return a === aggregate; }) ) ) {
    plv8.elog(ERROR, 'Invalid aggregate operation for population query: ', aggregate);
    return null;
  }

  if ( tstart >= tend ) {
    plv8.elog(ERROR, 'Invalid timestamps, tstart must be less than tend');
    return null;
  }


  try {

    ///////////////////////////////////////
    // Granola expression helpers.

    var granola_has_time_interval = 'body#>\'{body, effective_time_frame}\' ? \'time_interval\'';
    var granola_has_date_time = 'body#>\'{body, effective_time_frame}\' ? \'date_time\'';

    var granola_time_interval_start = 'body#>\'{body, effective_time_frame, time_interval, start_date_time}\'';
    var granola_date_time = 'body#>\'{body, effective_time_frame, date_time}\'';

    var granola_ts =
      '(case'
        + ' when ' + granola_has_time_interval + ' then ' + granola_time_interval_start
        + ' when ' + granola_has_date_time + ' then ' + granola_date_time
        + ' else null'
      + ' end)::text::timestamp';

    var granola_unix_ts = 'extract(epoch from ' + granola_ts + ')';

    var end_epoch = 'extract(epoch from ((body#>\'{body, effective_time_frame, time_interval, end_date_time}\')::text::timestamp))';
    var start_epoch = 'extract(epoch from ((body#>\'{body, effective_time_frame, time_interval, start_date_time}\')::text::timestamp))';

    var granola_duration =
      '(case'
        + ' when ' + granola_has_time_interval + ' then ' + end_epoch + ' - ' + start_epoch
        + ' else null end)::double precision';

    // Timestamp attribute template.
    var granola_timestamp_attr = 'extract(epoch from created_at)';
      /*
      `extract(epoch from
        (case when body#>'{body, effective_time_frame}' ? 'time_interval' then body#>'{body, effective_time_frame, time_interval, start_date_time}'
              when body#>'{body, effective_time_frame}' ? 'date_time' then body#>'{body, effective_time_frame, date_time}'
         else null end)::text::timestamp)`;
      */


    ///////////////////////////////////////
    // Schema mappings.

    var column_excludes = {
      "activity_type"  : true,
      "food_type"      : true
    };

    var filter_excludes = {
      "activity_type" : true,
      "food_type"     : true
    };

    var mc_meal_types = {
      'breakfast' : ['Breakfast', 'Bkfast'],
      'lunch'     : 'Lunch',
      'dinner'    : 'Dinner',
      'snack'     : 'Snack',
    };

    var mc_schema = {
      'body_weight'                        : {type: 'double precision',    relation: 'mc_body_measures'               , id: 0  },
      'body_height'                        : {type: 'double precision',    relation: 'mc_body_measures'               , id: 1  },
      'body_mass_index'                    : {type: 'double precision',    relation: 'mc_body_measures'               , id: 2  },
      'body_fat_percentage'                : {type: 'double precision',    relation: 'mc_body_measures'               , id: 3  },
      'body_temperature'                   : {type: 'double precision',    relation: 'mc_body_measures'               , id: 4  },
      'basal_body_temperature'             : {type: 'double precision',    relation: 'mc_body_measures'               , id: 5  },
      'lean_body_mass'                     : {type: 'double precision',    relation: 'mc_body_measures'               , id: 6  },
      'systolic_blood_pressure'            : {type: 'double precision',    relation: 'mc_blood_pressure_measures'     , id: 7  },
      'diastolic_blood_pressure'           : {type: 'double precision',    relation: 'mc_blood_pressure_measures'     , id: 8  },
      'sleep_duration'                     : {type: 'double precision',    relation: 'mc_sleep_measures'              , id: 9  },
      'meal_duration'                      : {type: 'double precision',    relation: 'mc_meal_measures'               , id: 10 },
      'food_type'                          : {type: 'jsonb',               relation: 'mc_meal_measures'               , id: 11 },
      'activity_duration'                  : {type: 'double precision',    relation: 'mc_activity_measures'           , id: 12 },
      'activity_type'                      : {type: 'integer',             relation: 'mc_activity_measures'           , id: 13 },
      'activity_value'                     : {type: 'jsonb',               relation: 'mc_activity_measures'           , id: 14 },
      'uv_exposure'                        : {type: 'double precision',    relation: 'mc_light_measures'              , id: 15 },
      'active_energy_burned'               : {type: 'double precision',    relation: 'mc_energy_measures'             , id: 16 },
      'basal_energy_burned'                : {type: 'double precision',    relation: 'mc_energy_measures'             , id: 17 },
      'blood_alcohol_content'              : {type: 'double precision',    relation: 'mc_blood_measures'              , id: 18 },
      'blood_glucose'                      : {type: 'double precision',    relation: 'mc_blood_measures'              , id: 19 },
      'blood_oxygen_saturation'            : {type: 'double precision',    relation: 'mc_blood_measures'              , id: 20 },
      'forced_expiratory_volume_one_second': {type: 'double precision',    relation: 'mc_lung_measures'               , id: 21 },
      'forced_vital_capacity'              : {type: 'double precision',    relation: 'mc_lung_measures'               , id: 22 },
      'peak_expiratory_flow'               : {type: 'double precision',    relation: 'mc_lung_measures'               , id: 23 },
      'inhaler_usage'                      : {type: 'double precision',    relation: 'mc_lung_measures'               , id: 24 },
      'respiratory_rate'                   : {type: 'double precision',    relation: 'mc_lung_measures'               , id: 25 },
      'heart_rate'                         : {type: 'double precision',    relation: 'mc_heart_rate_measures'         , id: 26 },
      'dietary_carbohydrates'              : {type: 'double precision',    relation: 'mc_nutrients_macro_measures'    , id: 27 },
      'dietary_energy_consumed'            : {type: 'double precision',    relation: 'mc_nutrients_macro_measures'    , id: 28 },
      'dietary_fat_total'                  : {type: 'double precision',    relation: 'mc_nutrients_macro_measures'    , id: 29 },
      'dietary_protein'                    : {type: 'double precision',    relation: 'mc_nutrients_macro_measures'    , id: 30 },
      'dietary_caffeine'                   : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 31 },
      'dietary_cholesterol'                : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 32 },
      'dietary_fat_monounsaturated'        : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 33 },
      'dietary_fat_polyunsaturated'        : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 34 },
      'dietary_fat_saturated'              : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 35 },
      'dietary_fiber'                      : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 36 },
      'dietary_sugar'                      : {type: 'double precision',    relation: 'mc_nutrients_subsets_measures'  , id: 37 },
      'dietary_calcium'                    : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 38 },
      'dietary_chloride'                   : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 39 },
      'dietary_chromium'                   : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 40 },
      'dietary_copper'                     : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 41 },
      'dietary_iodine'                     : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 42 },
      'dietary_iron'                       : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 43 },
      'dietary_magnesium'                  : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 44 },
      'dietary_manganese'                  : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 45 },
      'dietary_molybdenum'                 : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 46 },
      'dietary_niacin'                     : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 47 },
      'dietary_phosphorus'                 : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 48 },
      'dietary_potassium'                  : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 49 },
      'dietary_selenium'                   : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 50 },
      'dietary_sodium'                     : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 51 },
      'dietary_zinc'                       : {type: 'double precision',    relation: 'mc_nutrients_minerals_measures' , id: 52 },
      'dietary_biotin'                     : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 53 },
      'dietary_folate'                     : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 54 },
      'dietary_pantothenic_acid'           : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 55 },
      'dietary_riboflavin'                 : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 56 },
      'dietary_thiamin'                    : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 57 },
      'dietary_vitamina'                   : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 58 },
      'dietary_vitaminb12'                 : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 59 },
      'dietary_vitaminb6'                  : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 60 },
      'dietary_vitaminc'                   : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 61 },
      'dietary_vitamind'                   : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 62 },
      'dietary_vitamine'                   : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 63 },
      'dietary_vitamink'                   : {type: 'double precision',    relation: 'mc_nutrients_vitamins_measures' , id: 64 },
      'dietary_alcohol'                    : {type: 'double precision',    relation: 'mc_nutrients_liquids_measures'  , id: 65 },
      'dietary_water'                      : {type: 'double precision',    relation: 'mc_nutrients_liquids_measures'  , id: 66 },
      'apple_stand_hour'                   : {type: 'double precision',    relation: 'mc_misc_measures'               , id: 67 },
      'electrodermal_activity'             : {type: 'double precision',    relation: 'mc_misc_measures'               , id: 68 },
      'nike_fuel'                          : {type: 'double precision',    relation: 'mc_misc_measures'               , id: 69 },
      'number_of_times_fallen'             : {type: 'double precision',    relation: 'mc_misc_measures'               , id: 70 },
      'peripheral_perfusion_index'         : {type: 'double precision',    relation: 'mc_misc_measures'               , id: 71 }
    };

    var hk_activity_types = {
      'american_football'               : 'HKWorkoutActivityTypeAmericanFootball'                  ,
      'archery'                         : 'HKWorkoutActivityTypeArchery'                           ,
      'australian_football'             : 'HKWorkoutActivityTypeAustralianFootball'                ,
      'badminton'                       : 'HKWorkoutActivityTypeBadminton'                         ,
      'baseball'                        : 'HKWorkoutActivityTypeBaseball'                          ,
      'basketball'                      : 'HKWorkoutActivityTypeBasketball'                        ,
      'bowling'                         : 'HKWorkoutActivityTypeBowling'                           ,
      'boxing'                          : 'HKWorkoutActivityTypeBoxing'                            ,
      'climbing'                        : 'HKWorkoutActivityTypeClimbing'                          ,
      'cricket'                         : 'HKWorkoutActivityTypeCricket'                           ,
      'cross_training'                  : 'HKWorkoutActivityTypeCrossTraining'                     ,
      'curling'                         : 'HKWorkoutActivityTypeCurling'                           ,
      'cycling'                         : 'HKWorkoutActivityTypeCycling'                           ,
      'dance'                           : 'HKWorkoutActivityTypeDance'                             ,
      'dance_inspired_training'         : 'HKWorkoutActivityTypeDanceInspiredTraining'             ,
      'elliptical'                      : 'HKWorkoutActivityTypeElliptical'                        ,
      'equestrian_sports'               : 'HKWorkoutActivityTypeEquestrianSports'                  ,
      'fencing'                         : 'HKWorkoutActivityTypeFencing'                           ,
      'fishing'                         : 'HKWorkoutActivityTypeFishing'                           ,
      'functional_strength_training'    : 'HKWorkoutActivityTypeFunctionalStrengthTraining'        ,
      'golf'                            : 'HKWorkoutActivityTypeGolf'                              ,
      'gymnastics'                      : 'HKWorkoutActivityTypeGymnastics'                        ,
      'handball'                        : 'HKWorkoutActivityTypeHandball'                          ,
      'hiking'                          : 'HKWorkoutActivityTypeHiking'                            ,
      'hockey'                          : 'HKWorkoutActivityTypeHockey'                            ,
      'hunting'                         : 'HKWorkoutActivityTypeHunting'                           ,
      'lacrosse'                        : 'HKWorkoutActivityTypeLacrosse'                          ,
      'martial_arts'                    : 'HKWorkoutActivityTypeMartialArts'                       ,
      'mind_and_body'                   : 'HKWorkoutActivityTypeMindAndBody'                       ,
      'mixed_metabolic_cardio_training' : 'HKWorkoutActivityTypeMixedMetabolicCardioTraining'      ,
      'paddle_sports'                   : 'HKWorkoutActivityTypePaddleSports'                      ,
      'play'                            : 'HKWorkoutActivityTypePlay'                              ,
      'preparation_and_recovery'        : 'HKWorkoutActivityTypePreparationAndRecovery'            ,
      'racquetball'                     : 'HKWorkoutActivityTypeRacquetball'                       ,
      'rowing'                          : 'HKWorkoutActivityTypeRowing'                            ,
      'rugby'                           : 'HKWorkoutActivityTypeRugby'                             ,
      'running'                         : 'HKWorkoutActivityTypeRunning'                           ,
      'sailing'                         : 'HKWorkoutActivityTypeSailing'                           ,
      'skating_sports'                  : 'HKWorkoutActivityTypeSkatingSports'                     ,
      'snow_sports'                     : 'HKWorkoutActivityTypeSnowSports'                        ,
      'soccer'                          : 'HKWorkoutActivityTypeSoccer'                            ,
      'softball'                        : 'HKWorkoutActivityTypeSoftball'                          ,
      'squash'                          : 'HKWorkoutActivityTypeSquash'                            ,
      'stair_climbing'                  : 'HKWorkoutActivityTypeStairClimbing'                     ,
      'surfing_sports'                  : 'HKWorkoutActivityTypeSurfingSports'                     ,
      'swimming'                        : 'HKWorkoutActivityTypeSwimming'                          ,
      'table_tennis'                    : 'HKWorkoutActivityTypeTableTennis'                       ,
      'tennis'                          : 'HKWorkoutActivityTypeTennis'                            ,
      'track_and_field'                 : 'HKWorkoutActivityTypeTrackAndField'                     ,
      'traditional_strength_training'   : 'HKWorkoutActivityTypeTraditionalStrengthTraining'       ,
      'volleyball'                      : 'HKWorkoutActivityTypeVolleyball'                        ,
      'walking'                         : 'HKWorkoutActivityTypeWalking'                           ,
      'water_fitness'                   : 'HKWorkoutActivityTypeWaterFitness'                      ,
      'water_polo'                      : 'HKWorkoutActivityTypeWaterPolo'                         ,
      'water_sports'                    : 'HKWorkoutActivityTypeWaterSports'                       ,
      'wrestling'                       : 'HKWorkoutActivityTypeWrestling'                         ,
      'yoga'                            : 'HKWorkoutActivityTypeYoga'                              ,
      'other'                           : 'HKWorkoutActivityTypeOther'                             ,
      'step_count'                      : 'HKQuantityTypeIdentifierStepCount'                      ,
      'distance_walking_running'        : 'HKQuantityTypeIdentifierDistanceWalkingRunning'         ,
      'flights_climbed'                 : 'HKQuantityTypeIdentifierFlightsClimbed'
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

    var mc_activity_value_quantities = {
      'distance'    : true,
      'step_count'  : true,
      'flights'     : true,
      'kcal_burned' : true
    };

    var nhanes_constant_measures = { 'uv_exposure'          : 12.0,
                                     'active_energy_burned' : 2750.0 };

    var nhanes_attr_tables = {
      'body_weight'                 : {'table': 'nhanes_body_measures',           'attr': 'body_weight*0.453592'        },
      'body_mass_index'             : {'table': 'nhanes_body_measures',           'attr': 'body_mass_index'             },
      'heart_rate'                  : {'table': 'nhanes_heart_rate_measures',     'attr': 'heart_rate'                  },
      'diastolic_blood_pressure'    : {'table': 'nhanes_blood_pressure_measures', 'attr': 'diastolic_blood_pressure'    },
      'systolic_blood_pressure'     : {'table': 'nhanes_blood_pressure_measures', 'attr': 'systolic_blood_pressure'     },
      'sleep_duration'              : {'table': 'nhanes_sleep_measures',          'attr': 'sleep'                       },
      'dietary_carbohydrates'       : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_carbohydrates'       },
      'dietary_caffeine'            : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_caffeine'            },
      'dietary_cholesterol'         : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_cholesterol'         },
      'dietary_energy_consumed'     : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_energy_consumed'     },
      'dietary_fat_total'           : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_fat_total'           },
      'dietary_fat_saturated'       : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_fat_saturated'       },
      'dietary_fat_monounsaturated' : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_fat_monounsaturated' },
      'dietary_fat_polyunsaturated' : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_fat_polyunsaturated' },
      'dietary_fiber'               : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_fiber'               },
      'dietary_protein'             : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_protein'             },
      'dietary_sodium'              : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_sodium'              },
      'dietary_sugar'               : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_sugar'               },
      'dietary_water'               : {'table': 'nhanes_24hr_nutrients_measures', 'attr': 'dietary_water'               }
    };

    var granola_unnamed_quantity = {
      'blood_alcohol_content'                : 'HKQuantityTypeIdentifierBloodAlcoholContent'       ,
      'diastolic_blood_pressure'             : 'HKQuantityTypeIdentifierBloodPressureDiastolic'    ,
      'dietary_alcohol'                      : 'HKQuantityTypeIdentifierDietaryAlcohol'            ,
      'dietary_biotin'                       : 'HKQuantityTypeIdentifierDietaryBiotin'             ,
      'dietary_caffeine'                     : 'HKQuantityTypeIdentifierDietaryCaffeine'           ,
      'dietary_calcium'                      : 'HKQuantityTypeIdentifierDietaryCalcium'            ,
      'dietary_carbohydrates'                : 'HKQuantityTypeIdentifierDietaryCarbohydrates'      ,
      'dietary_chloride'                     : 'HKQuantityTypeIdentifierDietaryChloride'           ,
      'dietary_cholesterol'                  : 'HKQuantityTypeIdentifierDietaryCholesterol'        ,
      'dietary_chromium'                     : 'HKQuantityTypeIdentifierDietaryChromium'           ,
      'dietary_copper'                       : 'HKQuantityTypeIdentifierDietaryCopper'             ,
      'dietary_energy_consumed'              : 'HKQuantityTypeIdentifierDietaryEnergyConsumed'     ,
      'dietary_fat_monounsaturated'          : 'HKQuantityTypeIdentifierDietaryFatMonounsaturated' ,
      'dietary_fat_polyunsaturated'          : 'HKQuantityTypeIdentifierDietaryFatPolyunsaturated' ,
      'dietary_fat_saturated'                : 'HKQuantityTypeIdentifierDietaryFatSaturated'       ,
      'dietary_fat_total'                    : 'HKQuantityTypeIdentifierDietaryFatTotal'           ,
      'dietary_fiber'                        : 'HKQuantityTypeIdentifierDietaryFiber'              ,
      'dietary_folate'                       : 'HKQuantityTypeIdentifierDietaryFolate'             ,
      'dietary_iodine'                       : 'HKQuantityTypeIdentifierDietaryIodine'             ,
      'dietary_iron'                         : 'HKQuantityTypeIdentifierDietaryIron'               ,
      'dietary_magnesium'                    : 'HKQuantityTypeIdentifierDietaryMagnesium'          ,
      'dietary_manganese'                    : 'HKQuantityTypeIdentifierDietaryManganese'          ,
      'dietary_molybdenum'                   : 'HKQuantityTypeIdentifierDietaryMolybdenum'         ,
      'dietary_niacin'                       : 'HKQuantityTypeIdentifierDietaryNiacin'             ,
      'dietary_pantothenic_acid'             : 'HKQuantityTypeIdentifierDietaryPantothenicAcid'    ,
      'dietary_phosphorus'                   : 'HKQuantityTypeIdentifierDietaryPhosphorus'         ,
      'dietary_potassium'                    : 'HKQuantityTypeIdentifierDietaryPotassium'          ,
      'dietary_protein'                      : 'HKQuantityTypeIdentifierDietaryProtein'            ,
      'dietary_riboflavin'                   : 'HKQuantityTypeIdentifierDietaryRiboflavin'         ,
      'dietary_selenium'                     : 'HKQuantityTypeIdentifierDietarySelenium'           ,
      'dietary_sodium'                       : 'HKQuantityTypeIdentifierDietarySodium'             ,
      'dietary_thiamin'                      : 'HKQuantityTypeIdentifierDietaryThiamin'            ,
      'dietary_sugar'                        : 'HKQuantityTypeIdentifierDietarySugar'              ,
      'dietary_vitamina'                     : 'HKQuantityTypeIdentifierDietaryVitaminA'           ,
      'dietary_vitaminb12'                   : 'HKQuantityTypeIdentifierDietaryVitaminB12'         ,
      'dietary_vitaminb6'                    : 'HKQuantityTypeIdentifierDietaryVitaminB6'          ,
      'dietary_vitaminc'                     : 'HKQuantityTypeIdentifierDietaryVitaminC'           ,
      'dietary_vitamind'                     : 'HKQuantityTypeIdentifierDietaryVitaminD'           ,
      'dietary_vitamine'                     : 'HKQuantityTypeIdentifierDietaryVitaminE'           ,
      'dietary_vitamink'                     : 'HKQuantityTypeIdentifierDietaryVitaminK'           ,
      'dietary_water'                        : 'HKQuantityTypeIdentifierDietaryWater'              ,
      'dietary_zinc'                         : 'HKQuantityTypeIdentifierDietaryZinc'               ,
      'electrodermal_activity'               : 'HKQuantityTypeIdentifierElectrodermalActivity'     ,
      'forced_expiratory_volume_one_second'  : 'HKQuantityTypeIdentifierForcedExpiratoryVolume1'   ,
      'forced_vital_capacity'                : 'HKQuantityTypeIdentifierForcedVitalCapacity'       ,
      'inhaler_usage'                        : 'HKQuantityTypeIdentifierInhalerUsage'              ,
      'lean_body_mass'                       : 'HKQuantityTypeIdentifierLeanBodyMass'              ,
      'nike_fuel'                            : 'HKQuantityTypeIdentifierNikeFuel'                  ,
      'number_of_times_fallen'               : 'HKQuantityTypeIdentifierNumberOfTimesFallen'       ,
      'peak_expiratory_flow'                 : 'HKQuantityTypeIdentifierPeakExpiratoryFlowRate'    ,
      'peripheral_perfusion_index'           : 'HKQuantityTypeIdentifierPeripheralPerfusionIndex'  ,
      'systolic_blood_pressure'              : 'HKQuantityTypeIdentifierBloodPressureSystolic'     ,
      'uv_exposure'                          : 'HKQuantityTypeIdentifierUVExposure'
    };

    var granola_named = {
      'body_height'              : 'body_height'              ,
      'body_weight'              : 'body_weight'              ,
      'heart_rate'               : 'heart_rate'               ,
      'blood_glucose'            : 'blood_glucose'            ,
      'active_energy_burned'     : 'kcal_burned'              ,
      'basal_energy_burned'      : 'kcal_burned'              ,
      'body_mass_index'          : 'body_mass_index'          ,
      'body_fat_percentage'      : 'body_fat_percentage'      ,
      'blood_oxygen_saturation'  : 'oxygen_saturation'        ,
      'respiratory_rate'         : 'respiratory_rate'         ,
      'body_temperature'         : 'body_temperature'         ,
      'basal_body_temperature'   : 'body_temperature'         ,
      'sleep_duration'           : 'sleep_duration'           ,
      'systolic_blood_pressure'  : 'systolic_blood_pressure'  ,
      'diastolic_blood_pressure' : 'diastolic_blood_pressure' ,
    };

    var granola_count_attrs = {
      'uv_exposure'            : true,
      'inhaler_usage'          : true,
      'nike_fuel'              : true,
      'number_of_times_fallen' : true,
    };

    // Note: we do not include 'sleep_duration' as an HKCategory, since all of our use sites
    // expect a disjoint schema mapping, and 'sleep_duration' is already an OMH attribute.
    var mc_to_hkcategory = {
      'apple_stand_hour' : { value : 'Standing',
                             hk    : 'HKCategoryTypeIdentifierAppleStandHour',
                             expr  : granola_duration
                           },
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

    //////////////////////////////////
    // Materialized view constants.

    // Meal-specific measure ids.

    // TODO: timed meal?
    var mc_meal_duration_ids = {
      'breakfast'   : 40001,
      'lunch'       : 40002,
      'dinner'      : 40003,
      'snack'       : 40004
    };

    // Aggregate activity value ids.
    var activity_distance_id = 50001;
    var activity_energy_id   = 50002;

    // Activity-specific measure id offsets.
    var activity_duration_id_offset = 10000;
    var activity_distance_id_offset = 20000;
    var activity_energy_id_offset   = 30000;

    // Quantity activitiy durations and values.
    var mc_activity_value_quantity_ids = {
      'step_count'      : { 'step_count' : 20058 },
      'flights_climbed' : { 'flights'    : 20060 }
    };


    ////////////////////////////////////
    // Categorized filter helpers.

    // Apply a function to an array of condition specs, descending into any
    // categorized specs as necessary.
    // The result is the flattened output of function applications.
    var map_all_conditions = function(conditions, map_cb, result_cb) {
      var apply_map_cb = function(partial, arg) {
        var map_cb_result = {};
        map_cb(arg, function(err, map_fn_result) {
          if ( err ) { map_cb_result = {status: err, partial: []}; }
          else { map_cb_result = {status: '', partial: partial.concat([map_fn_result])}; }
        });
        return map_cb_result;
      };

      var reduce_result = conditions.reduce(function (acc, cond_spec) {
        if ( !strIsEmpty(acc.status) ) { return acc; }
        if ( cond_spec.hasOwnProperty('category_filters') ) {
          return Object.keys(cond_spec.category_filters).reduce(function(acc, ct) {
            if ( !strIsEmpty(acc.status) ) { return acc; }
            return apply_map_cb(acc.partial, cond_spec.category_filters[ct]);
          }, acc);
        } else {
          return apply_map_cb(acc.partial, cond_spec);
        }
      }, {status: '', partial: []});

      if ( !strIsEmpty(reduce_result.status) ) {
        return result_cb(reduce_result.status);
      } else {
        return result_cb(null, reduce_result.partial);
      }
    };


    ////////////////////////////////////////////
    // Object classification and utilities.

    var isSingletonObject = function(obj) { return Object.keys(obj).length == 1; };

    var isMealOrActivityAttribute = function(attr) {
      return anyArray(['meal_duration', 'activity_duration', 'activity_value'], function(i) { return i === attr; });
    };

    var isPredicateObject = function(obj) {
      return anyArray(['min', 'max', 'value'], function(i) { return obj.hasOwnProperty(i); });
    };

    var isHavingCondition = function(spec) {
      return spec.hasOwnProperty('having_condition');
    };

    var allHavingConditions = function(spec) {
      return map_all_conditions([spec],
        function(cond_spec, cb) { cb(null, isHavingCondition(cond_spec) && !strIsEmpty(cond_spec.having_condition)); },
        function(err, map_result) {
          if (err) { return false; }
          return allArray(map_result, function(i) { return i; });
        }
      );
    };

    var isCategorizedConditionSpec = function(spec) {
      return spec.hasOwnProperty('category_filters');
    }


    //
    // Utilities.
    var onSingleton = function(obj, singleton_cb, err_cb) {
      if ( isSingletonObject(obj) ) {
        var key = Object.keys(obj)[0];
        singleton_cb(key, obj[key]);
      } else { err_cb(obj); }
    };

    var onStringOrSingleton = function(obj, string_cb, singleton_cb, err_cb) {
      if ( isString(obj) ) { string_cb(obj); }
      else if ( isSingletonObject(obj) ) {
        var key = Object.keys(obj)[0];
        singleton_cb(key, obj[key]);
      }
      else { err_cb(obj); }
    };

    var onStringOrPredicateOrSingleton = function(obj, string_cb, predicate_cb, singleton_cb, err_cb) {
      if ( isString(obj) ) { string_cb(obj); }
      else if ( isPredicateObject(obj) ) { predicate_cb(obj); }
      else if ( isSingletonObject(obj) ) {
        var key = Object.keys(obj)[0];
        singleton_cb(key, obj[key]);
      }
      else { err_cb(obj); }
    };


    //////////////////////////////////////////
    // Condition spec constructors.

    var mk_filter_condition = function(params, as_having) {
      // Condition extraction for all filter attributes.
      // Filters are inclusive of their boundary values.
      var condition = '';
      if ( params.hasOwnProperty('min') && params.hasOwnProperty('max') ) {
        condition = 'between ' + params.min + ' and ' + params.max;
      } else if ( params.hasOwnProperty('min') ) {
        condition = '>= ' + params.min;
      } else if ( params.hasOwnProperty('max') ) {
        condition = '<= ' + params.max;
      } else if ( params.hasOwnProperty('value') ) {
        var v = params.value;
        if ( attr_type == 'text') { v = "E'" + params.value + "'"; }
        else if ( attr_type == 'jsonb' ) { testVal = "'" + params.value + "'::jsonb"; }
        condition = ' = ' + v;
      }

      if ( as_having ) {
        return { having_condition: condition };
      } else {
        return { attr_condition: condition };
      }
    };

    var mk_columnfilter_spec = function(attr_path, params) {
      var mc_name = isArray(attr_path) ? attr_path[0] : attr_path;
      var spec_attr = ( isArray(attr_path) && attr_path.length == 1 ) ? attr_path[0] : attr_path;
      return extendObject({attr: spec_attr, cumulative: mc_attrs_acquisition_class[mc_name].cumulative}, mk_filter_condition(params, false));
    };

    // Example 1
    // Input: {running: {min: 0, max: 3600}, cycling: {min: 1800, max: 7200}}
    // Output: {running: {attr: [<attr>, running], having_condition: ...}, cycling: {attr: [<attr>, cycling], having_condition: ...}}
    //
    // Example 2
    // Input: {lunch: {min: 0, max: 3600}, dinner: {min: 1800, max: 7200}}
    // Output: {lunch: {attr: [<attr>, lunch], having_condition: ...}, dinner: {attr: [<attr>, dinner], having_condition: ...}}
    var mk_categorized_userfilter_spec = function (attr, spec_object) {
      var filter_specs_by_type = {};
      var invalid_userfilters = [];

      Object.keys(spec_object).forEach(function(category_type) {
        var category_spec = {attr: [attr, category_type], cumulative: mc_attrs_acquisition_class[attr].cumulative};
        var spec_extension = {};

        if ( attr === 'meal_duration' && mc_meal_duration_ids.hasOwnProperty(category_type) ) {
          spec_extension = {attr_id: mc_meal_duration_ids[category_type]};
        }
        else if ( attr === 'activity_duration' && mc_activity_types.hasOwnProperty(category_type) ) {
          spec_extension = {attr_id: Number(mc_activity_types[category_type]) + activity_duration_id_offset};
        }
        else if ( attr === 'activity_value' && mc_activity_types.hasOwnProperty(category_type) && spec_object[category_type].hasOwnProperty('quantity') ) {
          var quantity = spec_object[category_type].quantity;

          if ( mc_activity_value_quantity_ids.hasOwnProperty(category_type) && mc_activity_value_quantity_ids[category_type].hasOwnProperty(quantity) ) {
            spec_extension = {attr_id: mc_activity_value_quantity_ids[category_type][quantity]};
          } else if ( quantity === 'distance' ) {
            spec_extension = {attr_id: Number(mc_activity_types[category_type]) + activity_distance_id_offset};
          } else if ( quantity === 'kcal_burned' ) {
            spec_extension = {attr_id: Number(mc_activity_types[category_type]) + activity_energy_id_offset};
          }

          spec_extension = extendObject(spec_extension, {quantity: spec_object[category_type].quantity});
        }

        category_spec = extendObject(category_spec, spec_extension);
        if ( isPredicateObject(spec_object[category_type]) ) {
          filter_specs_by_type[category_type] = extendObject(category_spec, mk_filter_condition(spec_object[category_type], true));
        } else {
          invalid_userfilters.push(spec_object[category_type]);
        }
      });

      if ( !arrIsEmpty(invalid_userfilters) ) { return {invalid: invalid_userfilters}; }
      return filter_specs_by_type;
    };

    // Extracts a measure predicate from a MC REST API filter specification.
    // 'params' input may be a predicate represented as a min-max-value dictionary, e.g:
    // {min: 100, max: 200} or {value: 150}
    //
    // or a categorized predicate, e.g.:
    // {lunch: {min: 0, max: 3600}, dinner: {min: 1800, max: 7200}}
    //
    // Note: category filters for populations form a conjunction.
    var mk_userfilter_spec = function(attr, params) {
      if ( isMealOrActivityAttribute(attr) && !isPredicateObject(params) ) {
        return {attr: attr, category_filters: mk_categorized_userfilter_spec(attr, params)};
      } else if ( isPredicateObject(params) ) {
        // Handle uncategorized specs.
        // Note these may be for uncategorized meal/activity measures as well as any other measure.

        var attr_id = mc_schema[attr].id;
        var init_spec = {attr: attr, attr_id: attr_id, cumulative: mc_attrs_acquisition_class[attr].cumulative};

        // Handle attr_id selection for activity values based on quantity type.
        // Since this is an uncategorized filter spec, these quantities refer to an aggregate activity value across all activity types.
        if ( attr === 'activity_value' && params.hasOwnProperty('quantity') ) {
          if ( mc_activity_value_quantity_ids.hasOwnProperty(attr) && mc_activity_value_quantity_ids[attr].hasOwnProperty(params.quantity) ) {
            attr_id = mc_activity_value_quantity_ids[attr][params.quantity];
          } else if ( params.quantity === 'distance' ) {
            attr_id = activity_distance_id;
          } else if ( params.quantity === 'kcal_burned' ) {
            attr_id = activity_energy_id;
          }
          init_spec = extendObject(init_spec, {quantity: params.quantity, attr_id: attr_id});
        }

        return extendObject(init_spec, mk_filter_condition(params, true));
      } else {
        return {invalid: [params]};
      }
    };

    var mc_json_filter_extra_attrs = function(filter_spec) {
      var result = [isArray(filter_spec.attr) ? filter_spec.attr[0] : filter_spec.attr];
      if ( isCategorizedConditionSpec(filter_spec) ) {
        if ( filter_spec.attr === 'meal_duration' ) {
          result.push('food_type');
        }
        else if ( filter_spec.attr === 'activity_value' || filter_spec.attr === 'activity_duration' ) {
          result.push('activity_type');
        }
      }
      else if ( isArray(filter_spec.attr) && filter_spec.attr.length > 1 ) {
        if ( filter_spec.attr[0] === 'meal_duration' ) {
          result.push('food_type');
        } else if ( filter_spec.attr[0] === 'activity_value' || filter_spec.attr[0] == 'activity_duration' ) {
          result.push('activity_type');
        }
      }
      return result;
    };


    //////////////////////////////////
    // Parameter accessors.

    var attrOfColumnParam = function (param) {
      if ( isString(param) ) { return param; }
      else if ( isSingletonObject(param) ) { return Object.keys(param)[0]; }
      return null;
    };

    var attrNameOfFilterSpec = function(spec) {
      var name_fields = isArray(spec.attr) ? spec.attr : [spec.attr];
      return name_fields.concat(spec.hasOwnProperty('quantity') ? [spec.quantity] : []).join('_');
    };

    var attrTypeOfFilterSpec = function(spec) {
      var attr = isArray(spec.attr) ? spec.attr[0] : spec.attr;
      if ( spec.hasOwnProperty('quantity') ) {
        return 'double precision';
      }
      return mc_schema[attr].type;
    };

    var attrIdOfFilterSpec = function(spec) {
      var attr = isArray(spec.attr) ? spec.attr[0] : spec.attr;
      var measure_type = isArray(spec.attr) && spec.attr.length > 1 ? spec.attr[1] : '';

      if ( !strIsEmpty(measure_type) ) {
        if ( attr === 'meal_duration' && mc_meal_duration_ids.hasOwnProperty(measure_type) ) {
          return mc_meal_duration_ids[measure_type];
        }
        else if ( attr === 'activity_duration' && mc_activity_types.hasOwnProperty(measure_type) ) {
          return Number(mc_activity_types[measure_type]) + activity_duration_id_offset;
        }
        else if ( attr === 'activity_value' && mc_activity_types.hasOwnProperty(measure_type) && spec.hasOwnProperty('quantity') ) {
          if ( mc_activity_value_quantity_ids.hasOwnProperty(measure_type) && mc_activity_value_quantity_ids[measure_type].hasOwnProperty(spec.quantity) ) {
            return mc_activity_value_quantity_ids[measure_type][spec.quantity];
          } else if ( spec.quantity === 'distance' ) {
            return Number(mc_activity_types[measure_type]) + activity_distance_id_offset;
          } else if ( spec.quantity === 'kcal_burned' ) {
            return Number(mc_activity_types[measure_type]) + activity_energy_id_offset;
          }
        }
      } else {
        // Handle attr_id selection for activity values based on quantity type.
        // Since this is an uncategorized filter spec, these quantities refer to an aggregate activity value across all activity types.
        if ( attr === 'activity_value' && spec.hasOwnProperty('quantity') ) {
          if ( mc_activity_value_quantity_ids.hasOwnProperty(attr) && mc_activity_value_quantity_ids[attr].hasOwnProperty(spec.quantity) ) {
            return mc_activity_value_quantity_ids[attr][spec.quantity];
          } else if ( spec.quantity === 'distance' ) {
            return activity_distance_id;
          } else if ( spec.quantity === 'kcal_burned' ) {
            return activity_energy_id;
          }
        }
      }
      return mc_schema[attr].id;
    };

    var filterSpecHasAttrCondition = function(spec) {
      return spec.hasOwnProperty('attr_condition') && !strIsEmpty(spec.attr_condition);
    };

    var filterSpecOfColumnParam = function(obj) {
      var attr = attrOfColumnParam(obj);
      var filter_spec = {};
      var invalid_columns = [];

      onStringOrSingleton(obj,
        function(s) { filter_spec = {attr: s, cumulative: mc_attrs_acquisition_class[attr].cumulative}; },
        function(attr, measure_obj) {
          if ( attr === 'meal_duration' || attr === 'activity_duration' ) {
            // Meal/activity type, filter, e.g.,
            // a. Type only
            //    column[0][meal_duration] = lunch
            //
            // b. Coarsened filter.
            //    column[0][meal_duration][min] = 1800
            //    column[0][meal_duration][max] = 7200
            //
            // c. Type and filter.
            //    column[0][meal_duration][lunch][min] = 1800
            //    column[0][meal_duration][lunch][max] = 7200
            //
            var measure_type = '';

            onStringOrPredicateOrSingleton(measure_obj,
              function(s) { filter_spec = {attr: [attr, s], cumulative: mc_attrs_acquisition_class[attr].cumulative}; }, // Type only.
              function(param) { filter_spec = mk_columnfilter_spec([attr], param); }, // Coarsened predicate.
              function(measure_type, param) { filter_spec = mk_columnfilter_spec([attr, measure_type], param); }, // Type and filter.
              function(e) { invalid_columns.push(e); }
            );
          } else if ( attr === 'activity_value' ) {
            // Activity value type, quantity, filter, e.g.,
            // a. Coarsened quantity:
            //    column[0][activity_value] = distance
            //
            // a. Coarsened filter:
            //    column[0][activity_value][quantity] = distance
            //    column[0][activity_value][min] = 1000
            //    column[0][activity_value][max] = 5000
            //
            // a. Quantity only:
            //    column[0][activity_value][running] = distance
            //
            // b. Quantity and filter:
            //    column[0][activity_value][running][quantity] = distance
            //    column[0][activity_value][running][min] = 1000
            //    column[0][activity_value][running][max] = 5000
            //
            onStringOrPredicateOrSingleton(measure_obj,
              function(s) { filter_spec = {attr: attr, cumulative: mc_attrs_acquisition_class[attr].cumulative, quantity: s}; }, // Coarsened quantity.
              function(param) { // Coarsened predicate.
                if ( param.hasOwnProperty('quantity') ) {
                  filter_spec = extendObject({quantity: param.quantity}, mk_columnfilter_spec([attr], param));
                } else {
                  invalid_columns.push(param);
                }
              },
              function(activity_type, activity_obj) {
                if ( isString(activity_obj) && mc_activity_value_quantities.hasOwnProperty(activity_obj) ) {
                  // Quantity only.
                  filter_spec = {attr: [attr, activity_type], cumulative: mc_attrs_acquisition_class[attr].cumulative, quantity: activity_obj};
                } else if ( activity_obj.hasOwnProperty('quantity')
                              && mc_activity_value_quantities.hasOwnProperty(activity_obj.quantity)
                              && isPredicateObject(activity_obj) )
                {
                  // Quantity and filter.
                  var activity_filter = pickObject(activity_obj, ['min', 'max', 'value']);
                  filter_spec = extendObject({quantity: activity_obj.quantity}, mk_columnfilter_spec([attr, activity_type], activity_filter));
                } else {
                  invalid_columns.push(obj);
                }
              },
              function(e) { invalid_columns.push(e); }
            );
          } else if ( isPredicateObject(measure_obj) ) {
            // Column filter.
            //  column[0][body_weight][min] = 100
            //  column[0][body_weight][max] = 200
            var param = pickObject(measure_obj, ['min', 'max', 'value']);
            filter_spec = mk_columnfilter_spec([attr], param);
          } else {
            invalid_columns.push(obj);
          }
        },
        function (e) { invalid_columns.push(e); });

      return {attr: attr, spec: filter_spec, invalid_columns: invalid_columns};
    };


    /////////////////////////////////////
    // Data structure initialization.

    // Valid attributes for projection
    var valid_column_attrs = objectFromKV(Object.keys(mc_schema).filter(function (k) {
      return !column_excludes.hasOwnProperty(k);
    }).map(function (k) { return [k, true]; }));

    // Valid attributes for filtering
    var valid_filter_attrs = objectFromKV(Object.keys(mc_schema).filter(function (k) {
      return !filter_excludes.hasOwnProperty(k);
    }).map(function (k) { return [k, true]; }));

    // Condition specs for each query column.
    var column_specs = [];

    // The set of attributes beyond the columns requested that should be extracted from json measure records.
    var mc_json_schema_attrs = valid_column_attrs;

    // Invalid attributes found in either the requested columns or filters.
    var invalid_columns = [];
    var invalid_userfilters = [];

    // Extract projection columns.
    if ( parameters.hasOwnProperty('columns') ) {
      var columns = isArray(parameters.columns) ?
        parameters.columns : Object.keys(parameters.columns).map(function(k) { return parameters.columns[k]; });

      var try_column_specs = columns.map(function(obj) {
        var attr = attrOfColumnParam(obj);
        if ( attr && valid_column_attrs.hasOwnProperty(attr) ) {
          var fs = filterSpecOfColumnParam(obj);
          invalid_columns = invalid_columns.concat(fs.invalid_columns);
          return fs.spec;
        }
        return {invalid: obj};
      });

      var valid_invalid = partitionArray(try_column_specs, function(o) {
        return !o.hasOwnProperty('invalid');
      });

      column_specs = valid_invalid[0];
      invalid_columns = invalid_columns.concat(valid_invalid[1].map(function(o) { return o.invalid; }));

      mc_json_schema_attrs = objectFromKV(column_specs.map(function(col_spec) {
        return [(isArray(col_spec.attr) ? col_spec.attr[0] : col_spec.attr), true];
      }));
    } else {
      var column_attrs = Object.keys(valid_column_attrs).sort();

      if ( view_initializer ) {
        var category_excludes = {
          'meal_duration'     : true,
          'activity_duration' : true,
          'activity_value'    : true,
        };

        var quantity_activities = {
          'step_count': true,
          'flights_climbed': true,
          'distance_walking_running': true
        };

        var non_quantity_activities = Object.keys(mc_activity_types).filter(function(a) {
          return !quantity_activities.hasOwnProperty(a);
        });

        column_specs = column_attrs.filter(function(a) {
          return !category_excludes.hasOwnProperty(a);
        }).sort().map(function(attr) { return {attr: attr, cumulative: mc_attrs_acquisition_class[attr].cumulative}; });

        var cat_meal_duration_specs = [
          {attr: ['meal_duration', 'breakfast'] , cumulative: mc_attrs_acquisition_class['meal_duration'].cumulative},
          {attr: ['meal_duration', 'lunch']     , cumulative: mc_attrs_acquisition_class['meal_duration'].cumulative},
          {attr: ['meal_duration', 'dinner']    , cumulative: mc_attrs_acquisition_class['meal_duration'].cumulative},
          {attr: ['meal_duration', 'snack']     , cumulative: mc_attrs_acquisition_class['meal_duration'].cumulative}
        ];

        var cat_activity_duration_specs = Object.keys(mc_activity_types).map(function(activity) {
          return {attr: ['activity_duration', activity], cumulative: mc_attrs_acquisition_class['activity_duration'].cumulative};
        });

        var cat_activity_value_distance_specs = non_quantity_activities.map(function(activity) {
          return {attr: ['activity_value', activity], quantity: 'distance', cumulative: mc_attrs_acquisition_class['activity_value'].cumulative};
        });

        var cat_activity_value_energy_specs = non_quantity_activities.map(function(activity) {
          return {attr: ['activity_value', activity], quantity: 'kcal_burned', cumulative: mc_attrs_acquisition_class['activity_value'].cumulative};
        });

        var cat_quantity_activity_specs = [
          {attr: ['activity_value', 'step_count'], quantity: 'step_count', cumulative: mc_attrs_acquisition_class['activity_value'].cumulative},
          {attr: ['activity_value', 'flights_climbed'], quantity: 'flights', cumulative: mc_attrs_acquisition_class['activity_value'].cumulative},
          {attr: ['activity_value', 'distance_walking_running'], quantity: 'distance', cumulative: mc_attrs_acquisition_class['activity_value'].cumulative},
        ];

        column_specs = column_specs.concat(
          cat_meal_duration_specs, cat_activity_duration_specs,
          cat_activity_value_distance_specs, cat_activity_value_energy_specs, cat_quantity_activity_specs);

      } else {
        column_specs = column_attrs.map(function(attr) {
          return extendObject({attr: attr, cumulative: mc_attrs_acquisition_class[attr].cumulative}, attr === 'activity_value' ? {quantity: 'kcal_burned'} : {});
        });
      }
      mc_json_schema_attrs = valid_column_attrs;
    }

    // Early termination on erroneous columns or filter extraction.
    if ( !arrIsEmpty(invalid_columns) ) {
      plv8.elog(ERROR, 'Invalid column parameters: ', JSON.stringify(invalid_columns));
      return null;
    }

    if ( arrIsEmpty(column_specs) ) {
      plv8.elog(ERROR, 'No column attributes specified');
      return null;
    }

    // plv8.elog(WARNING, 'Columns', column_specs.map(function(s) { return isArray(s) ? s.attr.join('_') : s.attr; }).join(', '));

    ////////////////////////////
    // Query declarations.
    var nhanes_constant_queries = [];
    var nhanes_queries = {};
    var granola_queries = [];
    var mc_json_queries = [];
    var mc_queries = {};

    // Flag to indicate whether we have any column filters.
    var has_column_filters = false;

    // Query generation for output attributes.
    column_specs.forEach(function(filter_spec) {

      var attr = isArray(filter_spec.attr) ? filter_spec.attr[0] : filter_spec.attr;
      var uniq_attr = attrNameOfFilterSpec(filter_spec);

      has_column_filters = has_column_filters || filterSpecHasAttrCondition(filter_spec);

      if ( !(strIsEmpty(attr) || strIsEmpty(uniq_attr)) ) {

        // NHANES query.
        if ( use_nhanes ) {
          if ( nhanes_constant_measures.hasOwnProperty(attr) ) {
            nhanes_constant_queries.push({
              attr: attr,
              val: nhanes_constant_measures[attr].toString(),
            });
          }
          else if ( nhanes_attr_tables.hasOwnProperty(attr) ) {
            var tbl_name  = nhanes_attr_tables[attr]['table'];
            if ( tbl_name && tbl_name.length > 0 ) {
              var clone_spec = JSON.parse(JSON.stringify(filter_spec));
              if ( nhanes_queries.hasOwnProperty(tbl_name) ) {
                nhanes_queries[tbl_name].push(clone_spec);
              } else {
                nhanes_queries[tbl_name] = [clone_spec];
              }
            } else {
              nhanes_constant_queries.push({attr: uniq_attr, val: '0.0'});
            }
          } else {
            nhanes_constant_queries.push({attr: uniq_attr, val: '0.0'});
          }
        }

        // Granola query.
        // Aggregation expressions and match conditions will be generated later.
        if ( use_granola ) {
          granola_queries.push(JSON.parse(JSON.stringify(filter_spec)));
        }

        // MC-JSON measure aggregate query.
        if ( use_mc_json ) {
          mc_json_queries.push(JSON.parse(JSON.stringify(filter_spec)));

          // Add any filtered keys to the json schema extracted from measure records.
          // No need to add this attribute itself, since it has already been handled with the request columns.
          var extra_attrs = mc_json_filter_extra_attrs(filter_spec);
          extra_attrs.forEach(function(i) {
            if ( !mc_json_schema_attrs.hasOwnProperty(i) ) {
              mc_json_schema_attrs[i] = true;
            }
          });
        }

        // MC measure aggregate query.
        if ( use_mc ) {
          var relation = mc_schema[attr].relation;
          var clone_spec = JSON.parse(JSON.stringify(filter_spec));
          if ( mc_queries.hasOwnProperty(relation) ) {
            mc_queries[relation].push(clone_spec);
          } else {
            mc_queries[relation] = [clone_spec];
          }
        }
      }
    });

    // plv8.elog(WARNING, 'Completed query generator metadata extraction');

    //////////////////////////////////
    // Materialized view usage.

    // Set whether we use views once we have checked for the presence of column filters.
    var use_mc_view = !view_initializer && aggregate === 'avg' && !has_column_filters;


    //////////////////////////////////
    // Population filter extraction

    // Filter conditions.
    var nhanes_conjuncts = {};
    var granola_conjuncts = [];
    var mc_json_conjuncts = [];
    var mc_conjuncts = {};
    var mc_view_conjuncts = [];

    // TODO: switch to userfilter alone once API changes have been propagated.
    var userfilter = parameters.hasOwnProperty('filter') ? parameters.filter : parameters.userfilter;

    if ( userfilter ) {
      Object.keys(userfilter).forEach(function (key) {
        var val = userfilter[key];
        if ( valid_filter_attrs.hasOwnProperty(key) ) {
          var attr_type = mc_schema[key].type;

          // User filter extraction, including categorized types.
          var filter_spec = mk_userfilter_spec(key, val);

          if ( !filter_spec || filter_spec.hasOwnProperty('invalid') ) {
            invalid_userfilters.push(key);
          }
          else if ( allHavingConditions(filter_spec) ) {
            // NHANES user filtering.
            // No row conditions needed since measures are named attributes.
            // We do not perform any meal or activity-based filtering for NHANES due to the lack of suitable data.
            if ( use_nhanes ) {
              if ( nhanes_attr_tables.hasOwnProperty(key) ) {
                var tbl_name  = nhanes_attr_tables[key]['table'];
                if ( tbl_name && !strIsEmpty(tbl_name) ) {
                  var clone_spec = JSON.parse(JSON.stringify(filter_spec));
                  if ( nhanes_conjuncts.hasOwnProperty(tbl_name) ) {
                    nhanes_conjuncts[tbl_name].push(clone_spec);
                  } else {
                    nhanes_conjuncts[tbl_name] = [clone_spec];
                  }
                }
              } else {
                // We add any unknown attributes as constant queries, so that
                // we include this measure in our schema for our union query across all datasets.
                nhanes_constant_queries.push(extendObject(filter_spec, {val: '0.0'}));
              }
            }

            // Granola user filtering
            // Aggregation expressions and match conditions will be generated later.
            if ( use_granola ) {
              granola_conjuncts.push(JSON.parse(JSON.stringify(filter_spec)));
            }

            // MC-JSON filtering.
            if ( use_mc_json ) {
              mc_json_conjuncts.push(JSON.parse(JSON.stringify(filter_spec)));

              // Add any filtered keys to the json schema extracted from measure records.
              var extra_attrs = mc_json_filter_extra_attrs(filter_spec);
              extra_attrs.forEach(function(i) {
                if ( !mc_json_schema_attrs.hasOwnProperty(i) ) {
                  mc_json_schema_attrs[i] = true;
                }
              });
            }

            // MC filtering.
            // No row conditions needed since measures are named attributes.
            if ( use_mc ) {
              var relation = mc_schema[key].relation;
              var clone_spec = JSON.parse(JSON.stringify(filter_spec));
              if ( mc_conjuncts.hasOwnProperty(relation) ) {
                mc_conjuncts[relation].push(clone_spec);
              } else {
                mc_conjuncts[relation] = [clone_spec];
              }
            }

            if ( use_mc_view ) {
              var clone_spec = JSON.parse(JSON.stringify(filter_spec));
              mc_view_conjuncts.push(clone_spec);
            }

          }
        } else {
          invalid_userfilters.push(key);
        }
      });
    }

    // plv8.elog(WARNING, 'Completed userfilter extraction');

    // Early termination on erroneous filter extraction.
    if ( !arrIsEmpty(invalid_userfilters) ) {
      plv8.elog(ERROR, 'Invalid filter attributes: ' + invalid_userfilters.join(', '));
      return null;
    }

    /////////////////////////////
    // SQL generation utilities.
    var query_construction_errors = [];

    // Returns a value-on-a-predicate expression.
    // Input: condition_spec : {agg_expr: ., attr_condition: ., row_condition: .}
    var mk_condition_expr = function(indicator, condition_spec) {
      var expr = condition_spec.agg_expr
      var zero_expr = '0.0';

      var conjuncts = [];
      var then_expr = indicator ? '1.0' : expr;

      if ( !strIsEmpty(condition_spec.attr_condition) ) {
        conjuncts.push(expr + ' ' + condition_spec.attr_condition);
      }

      if ( !strIsEmpty(condition_spec.row_condition) ) {
        conjuncts.push(condition_spec.row_condition);
      }

      if ( arrIsEmpty(conjuncts) ) {
        return then_expr;
      } else {
        var case_condition = conjuncts.join(' and ');
        var case_expr = '(case when ' + case_condition + ' then ' + then_expr + ' else ' + zero_expr +  ' end)';
        return case_expr;
      }
    };

    // Returns an aggregation-on-a-predicate expression.
    // Input: condition_spec : {agg_expr: ., attr_condition: ., row_condition: .}
    var mk_condition_aggregator = function(aggregate, condition_spec) {
      var agg_expr = condition_spec.agg_expr
      var agg_zero_expr = (anyArray(['count', 'avg', 'sum'], function(i) { return aggregate === i; })) ?
                            '0.0' : (aggregate === 'min' ? '\'infinity\'' : '\'-infinity\'');

      var conjuncts = [];
      var then_expr = aggregate === 'count' ? '1.0' : agg_expr;

      if ( !strIsEmpty(condition_spec.attr_condition) ) {
        conjuncts.push(agg_expr + ' ' + condition_spec.attr_condition);
      }

      if ( !strIsEmpty(condition_spec.row_condition) ) {
        conjuncts.push(condition_spec.row_condition);
      }

      if ( arrIsEmpty(conjuncts) ) {
        return aggregate + '(' + agg_expr + ')';
      } else {
        var case_condition = conjuncts.join(' and ');
        var case_expr = 'case when ' + case_condition + ' then ' + then_expr + ' else ' + agg_zero_expr + ' end';
        return (aggregate === 'count' ? 'sum' : aggregate) + '(' + case_expr + ')';
      }
    };


    /////////////////////////////////////////////
    // Expression and predicate constructors.
    var mk_nhanes_agg_expr = function(cond_spec, cb) {
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      if ( nhanes_attr_tables.hasOwnProperty(attr) ) {
        cb(null, nhanes_attr_tables[attr]['attr']);
      } else {
        cb(null, attrNameOfFilterSpec(cond_spec));
      }
    }

    var mk_nhanes_condition_exprs = function(cond_spec, cb) {
      mk_nhanes_agg_expr(cond_spec, function(err, expr) {
        if (err) { cb(err); }
        else { cb(null, extendObject(cond_spec, { agg_expr: expr })); }
      });
    };

    var mk_granola_agg_expr = function(cond_spec, cb) {
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      var measure_type = (isArray(cond_spec.attr) && cond_spec.attr.length > 1) ? cond_spec.attr[1] : null;

      if ( attr === 'meal_duration' || attr === 'activity_duration' ) {
        // TODO: activity_duration should also include durations of quantity activities: step_count/flights_climbed/distance_walking_running
        cb(null, '(body#>>\'{body, duration, value}\')::double precision');
      }
      else if ( attr === 'activity_value' && cond_spec.hasOwnProperty('quantity') ) {
        if ( !measure_type || measure_type === 'all' ) {
          cb(null, '(coalesce(body#>>\'{body, step_count}\', body#>>\'{body, count}\', body#>>\'{body, ' + cond_spec.quantity + ', value}\'))::double precision');
        }
        // Step count is a named quantity called 'step_count' (see omh:step-count:1.x)
        else if ( measure_type === 'step_count' ) {
          cb(null, '(body#>>\'{body, step_count}\')::double precision');
        }
        else if ( measure_type === 'flights_climbed' ) {
          cb(null, '(body#>>\'{body, count}\')::double precision');
        }
        else {
          cb(null, '(body#>>\'{body, ' + cond_spec.quantity + ', value}\')::double precision');
        }
      }
      else if ( granola_unnamed_quantity.hasOwnProperty(attr) ) {
        var json_path = granola_count_attrs.hasOwnProperty(attr) ? 'body, count' : 'body, unit_value, value';
        cb(null, '(body#>>\'{' + json_path + '}\')::double precision');
      }
      else if ( granola_named.hasOwnProperty(attr) ) {
        cb(null, '(body#>>\'{body, ' + granola_named[attr] + ', value}\')::double precision');
      }
      else if ( mc_to_hkcategory.hasOwnProperty(attr) ) {
        cb(null, mc_to_hkcategory[attr].hasOwnProperty('expr') ?
                    mc_to_hkcategory[attr].expr : 'body#>>\'{body, category_value}\'');
      }
      else {
        cb('Invalid granola aggregation expression spec: ' + JSON.stringify(cond_spec));
      }
    };

    var mk_granola_match_attr_expr = function(cond_spec, cb) {
      var status = '';
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      var measure_type = (isArray(cond_spec.attr) && cond_spec.attr.length > 1) ? cond_spec.attr[1] : null;
      var row_conjuncts = strIsEmpty(cond_spec.row_condition) ? [] : [cond_spec.row_condition];

      if ( attr === 'meal_duration' ) {
        row_conjuncts.push('body#>>\'{body, activity_name}\' = E\'HKWorkoutActivityTypePreparationAndRecovery\'');
        if ( !measure_type || measure_type === 'all' ) {
          // Match meals, but not other preparation and recovery samples.
          row_conjuncts.push('body#>>\'{body, metadata, 0, key}\' in (E\'Meal Type\', E\'Source\')');
        }
        else if ( measure_type && mc_meal_types.hasOwnProperty(measure_type) ) {
          var food_type_rhs = '';
          var meal_types = mc_meal_types[measure_type];
          if ( isArray(meal_types) ) {
            food_type_rhs = 'in ( ' + meal_types.map(function(i) { return 'E\'' + i + '\''; }).join(', ') + ' )';
          } else {
            food_type_rhs = '= E\'' + meal_types + '\'';
          }
          row_conjuncts.push('body#>>\'{body, metadata, 0, value}\' ' + food_type_rhs);
        } else {
          status = 'Invalid meal duration type: ' + measure_type;
        }
      }
      else if ( attr === 'activity_duration' || attr === 'activity_value' ) {
        if ( !measure_type || measure_type === 'all' ) {
          // Match every non-meal activity.
          row_conjuncts.push('((body->\'body\' ? \'activity_name\')'
                              + ' or (body->\'body\' ? \'step_count\')'
                              + ' or (body#>>\'{body, quantity_type}\' = E\'HKQuantityTypeIdentifierFlightsClimbed\')'
                              + ' or (body#>>\'{body, quantity_type}\' = E\'HKQuantityTypeIdentifierDistanceWalkingRunning\'))');
          row_conjuncts.push('((body#>\'{body, metadata}\' is null) or (body#>>\'{body, metadata, 0, key}\' not in (E\'Meal Type\', E\'Source\')))');
        }
        else if ( measure_type && hk_activity_types.hasOwnProperty(measure_type) ) {
          var hk_activity_type = hk_activity_types[measure_type];
          var quantity_activities = ['step_count', 'distance_walking_running', 'flights_climbed'];

          if ( anyArray(quantity_activities, function (i) { return i === measure_type; }) ) {
            // Filter on a specific quantity.
            if ( measure_type === 'step_count' ) {
              row_conjuncts.push('body->\'body\' ? \'step_count\'');
            } else {
              row_conjuncts.push('body#>>\'{body, quantity_type}\' = E\'' + hk_activity_type + '\'');
            }
          } else {
            // Filter for a specific activity type (e.g., swimming)
            row_conjuncts.push('body#>>\'{body, activity_name}\' = E\'' + hk_activity_type + '\'');
          }
        }
        else {
          status = 'Invalid ' + (attr === 'activity_duration' ? 'activity duration' : 'activity value') + ' type: ' + measure_type;
        }
      }
      else if ( granola_unnamed_quantity.hasOwnProperty(attr) ) {
        row_conjuncts.push('(body#>>\'{body, quantity_type}\' = E\'' + granola_unnamed_quantity[attr] + '\')');
      }
      else if ( granola_named.hasOwnProperty(attr) ) {
        row_conjuncts.push('(body->\'body\' ? \'' + granola_named[attr] + '\')');
      }
      else if ( mc_to_hkcategory.hasOwnProperty(attr) ) {
        // TODO: use category value in mc_to_hkcategory in filter.
        row_conjuncts.push('(body#>>\'{body, category_type}\' = E\'' + mc_to_hkcategory[attr].hk + '\')');
      }
      else {
        status = 'Invalid granola match expression spec: ' + JSON.stringify(cond_spec);
      }

      if ( !strIsEmpty(status) ) {
        cb(status);
      } else {
        cb(null, arrIsEmpty(row_conjuncts) ? '' : ( '( ' + row_conjuncts.join(' and ') + ' )' ));
      }
    };

    // Returns an aggregation expression and an measure match predicate for a condition specification.
    var mk_granola_condition_exprs = function(cond_spec, cb) {
      mk_granola_agg_expr(cond_spec, function(err, agg_expr) {
        if ( err ) { cb(err); }
        else {
          mk_granola_match_attr_expr(cond_spec, function(err, match_expr) {
            if ( err ) { cb(err); }
            else { cb(null, extendObject(cond_spec, { agg_expr: agg_expr, row_condition: match_expr })); }
          });
        }
      });
    };

    var mk_mc_json_agg_expr = function(cond_spec, cb) {
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      var measure_type = isArray(cond_spec.attr) ? cond_spec.attr[1] : null;
      var quantity_activities = ['step_count', 'distance_walking_running', 'flights_climbed'];

      if ( attr === 'activity_value' && cond_spec.hasOwnProperty('quantity')  ) {
        var simple_value = 'activity_value#>>\'{' + cond_spec.quantity + '}\'';
        var nested_value = '((activity_value#>>\'{' + cond_spec.quantity + '}\')::jsonb)#>>\'{value}\'';

        var is_quantity_activity = anyArray(quantity_activities, function (i) { return i === measure_type; });

        if ( !measure_type ) {
          cb(null, '(coalesce(' + nested_value + ', ' + simple_value + '))::double precision');
        }
        else if ( measure_type && !is_quantity_activity ) {
          cb(null, '(' + nested_value + ')::double precision');
        }
        else {
          cb(null,  '(' + simple_value + ')::double precision');
        }
      }
      else if ( attr === 'activity_value' ) {
        cb('No quantity field found for MC-JSON activity value aggregation');
      }
      else {
        cb(null, attr);
      }
    };

    var mk_mc_json_match_attr_expr = function(cond_spec, cb) {
      var status = '';
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      var measure_type = (isArray(cond_spec.attr) && cond_spec.attr.length > 1) ? cond_spec.attr[1] : null;
      var row_conjuncts = strIsEmpty(cond_spec.row_condition) ? [] : [cond_spec.row_condition];

      row_conjuncts.push(attr + ' is not null');

      if ( attr === 'meal_duration' ) {
        if ( measure_type && mc_meal_types.hasOwnProperty(measure_type) ) {
          var food_type_rhs = '';
          var meal_types = mc_meal_types[measure_type];
          if ( isArray(meal_types) ) {
            food_type_rhs = 'in ( ' + meal_types.map(function(i) { return 'E\'' + i + '\''; }).join(', ') + ' )';
          } else {
            food_type_rhs = '= E\'' + meal_types + '\'';
          }
          row_conjuncts.push('((food_type is null) or (food_type#>>\'{value}\' ' + food_type_rhs + '))');
        } else if ( !measure_type || measure_type === 'all' ) {
          // No-op
        }
        else {
          status = 'Invalid meal duration type: ' + measure_type;
        }
      }
      else if ( attr === 'activity_duration' || attr === 'activity_value' ) {
        if ( measure_type && mc_activity_types.hasOwnProperty(measure_type) ) {
          var mc_activity_code = mc_activity_types[measure_type];
          row_conjuncts.push('((activity_type is null) or (activity_type = ' + mc_activity_code + '))');
        } else if ( !measure_type || measure_type === 'all' ) {
          // No-op.
        } else {
          status = 'Invalid ' + (attr === 'activity_duration' ? 'activity duration' : 'activity value') + ' type: ' + measure_type;
        }
      }

      if ( !strIsEmpty(status) ) {
        cb(status);
      } else {
        cb(null, arrIsEmpty(row_conjuncts) ? '' : ( '( ' + row_conjuncts.join(' and ') + ' )' ));
      }
    };

    // Returns an aggregation expression and an measure match predicate for a condition specification.
    var mk_mc_json_condition_exprs = function(cond_spec, cb) {
      mk_mc_json_agg_expr(cond_spec, function(err, agg_expr) {
        if ( err ) { cb(err); }
        else {
          mk_mc_json_match_attr_expr(cond_spec, function(err, match_expr) {
            if ( err ) { cb(err); }
            else { cb(null, extendObject(cond_spec, { agg_expr: agg_expr, row_condition: match_expr })); }
          });
        }
      });
    };


    var mk_mc_agg_expr = function(cond_spec, cb) {
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      var measure_type = isArray(cond_spec.attr) ? cond_spec.attr[1] : null;
      var quantity_activities = ['step_count', 'distance_walking_running', 'flights_climbed'];

      if ( attr === 'activity_value' && cond_spec.hasOwnProperty('quantity')  ) {
        var simple_value = 'activity_value#>>\'{' + cond_spec.quantity + '}\'';
        var nested_value = '((activity_value#>>\'{' + cond_spec.quantity + '}\')::jsonb)#>>\'{value}\'';

        var is_quantity_activity = anyArray(quantity_activities, function (i) { return i === measure_type; });

        if ( !measure_type ) {
          cb(null, '(coalesce(' + nested_value + ', ' + simple_value + '))::double precision');
        }
        else if ( measure_type && !is_quantity_activity ) {
          cb(null, '(' + nested_value + ')::double precision');
        }
        else {
          cb(null, '(' + simple_value + ')::double precision');
        }
      }
      else if ( attr === 'activity_value' ) {
        cb('No quantity field found for MC-Native activity value aggregation');
      }
      else {
        cb(null, attr);
      }
    };

    var mk_mc_match_attr_expr = function(cond_spec, cb) {
      var status = '';
      var attr = isArray(cond_spec.attr) ? cond_spec.attr[0] : cond_spec.attr;
      var measure_type = (isArray(cond_spec.attr) && cond_spec.attr.length > 1) ? cond_spec.attr[1] : null;
      var row_conjuncts = strIsEmpty(cond_spec.row_condition) ? [] : [cond_spec.row_condition];

      row_conjuncts.push(attr + ' is not null');

      if ( attr === 'meal_duration' ) {
        if ( measure_type && mc_meal_types.hasOwnProperty(measure_type) ) {
          var food_type_rhs = '';
          var meal_types = mc_meal_types[measure_type];
          if ( isArray(meal_types) ) {
            food_type_rhs = 'in ( ' + meal_types.map(function(i) { return 'E\'' + i + '\''; }).join(', ') + ' )';
          } else {
            food_type_rhs = '= E\'' + meal_types + '\'';
          }
          row_conjuncts.push('food_type#>>\'{0, value}\' ' + food_type_rhs);
        } else if ( !measure_type || measure_type === 'all' ) {
          // No-op
        }
        else {
          status = 'Invalid meal duration type: ' + measure_type;
        }
      }
      else if ( attr === 'activity_duration' || attr === 'activity_value' ) {
        if ( measure_type && mc_activity_types.hasOwnProperty(measure_type) ) {
          var mc_activity_code = mc_activity_types[measure_type];
          row_conjuncts.push('((activity_type is null) or (activity_type = ' + mc_activity_code + '))');
        } else if ( !measure_type || measure_type === 'all' ) {
          // No-op.
        } else {
          status = 'Invalid ' + (attr === 'activity_duration' ? 'activity duration' : 'activity value') + ' type: ' + measure_type;
        }
      }

      if ( !strIsEmpty(status) ) {
        cb(status);
      } else {
        cb(null, arrIsEmpty(row_conjuncts) ? '' : ( '( ' + row_conjuncts.join(' and ') + ' )' ));
      }
    };

    // Returns an aggregation expression and an measure match predicate for a condition specification.
    var mk_mc_condition_exprs = function(cond_spec, cb) {
      mk_mc_agg_expr(cond_spec, function(err, agg_expr) {
        if ( err ) { cb(err); }
        else {
          mk_mc_match_attr_expr(cond_spec, function(err, match_expr) {
            if ( err ) { cb(err); }
            cb(null, extendObject(cond_spec, { agg_expr: agg_expr, row_condition: match_expr }));
          });
        }
      });
    };

    var mk_mc_all_attrs = function(tbl_specs) {
      return uniqueArray(flattenArray(Object.keys(tbl_specs).map(function(relation) {
        return map_all_conditions(tbl_specs[relation],
          function(cond_spec, cb) { mk_mc_condition_exprs(cond_spec, cb); },
          function(err, map_result) {
            if (err) { query_construction_errors.push(err); return []; }
            return map_result;
          }
        ).map(function(cond_spec) {
          return attrNameOfFilterSpec(cond_spec);
        });
      }))).sort();
    };


    // Utility schema definitions.
    var nhanes_granola_schema = column_specs.map(function(col_spec) {
      var uniq_attr = attrNameOfFilterSpec(col_spec);
      var attr_type = attrTypeOfFilterSpec(col_spec);
      var ftypes = [uniq_attr + ' ' + attr_type];
      if ( aggregate === 'avg' ) {
        ftypes.push(uniq_attr + '_count bigint');
      }
      return ftypes.join(', ');
    }).sort().join(', ');

    var mc_json_row_schema = Object.keys(mc_json_schema_attrs).sort().map(function(attr) {
      return attr + ' ' + mc_schema[attr].type;
    }).join(', ');


    ////////////////////////////////////
    // Population query construction.

    var population_userids = [];
    if ( with_explicit_population ) {
      var population_inputs = isArray(parameters.population) ? parameters.population : [parameters.population];
      var valid_invalid_userids = partitionArray(population_inputs, function(u) { return /^[A-Za-z0-9]*$/.test(u); });

      // Error on empty or invalid user ids.
      if ( arrIsEmpty(valid_invalid_userids[0]) ) {
        plv8.elog(ERROR, 'Empty explicit population userids');
        return null;
      }

      if ( !arrIsEmpty(valid_invalid_userids[1]) ) {
        plv8.elog(ERROR, 'Invalid explicit population userid: ' + valid_invalid_userids[1].join(', '));
        return null;
      }

      population_userids = valid_invalid_userids[0].map(function(u) { return 'E\'' + u + '\''; });
    }

    // Early termination for errors in building population queries.
    if ( !arrIsEmpty(query_construction_errors) ) {
      plv8.elog(ERROR, 'Failed to build population queries:\n' + query_construction_errors.join('\n'));
      return null;
    }


    //////////////////////////////////////
    // View initialization helpers.

    var mk_view_measure_queries = function(measure_conditions, query_gen) {
      return map_all_conditions(measure_conditions,
        function(cond_spec, cb) {
          var uniq_attr = attrNameOfFilterSpec(cond_spec);
          var attr_id = attrIdOfFilterSpec(cond_spec).toString();
          query_gen(null, cond_spec.cumulative, uniq_attr, attr_id, cb);
        },
        function(err, map_result) {
          if (err) { query_construction_errors.push(err); return []; }
          return map_result;
        }
      );
    };

    var mk_user_day_view_measure_queries = function(measure_conditions) {
      return mk_view_measure_queries(measure_conditions, function(err, cumulative, uniq_attr, attr_id, cb) {
        if (err) { cb(err); } else {
          if ( cumulative ) {
            cb(null, 'select ' + attr_id + ' as msid, udbid, day, ' + uniq_attr + ' as msum, ' + uniq_attr + '_count as mcnt from user_day_view');
          } else {
            cb(null, '');
          }
        }
      }).filter(function(s) { return !strIsEmpty(s); }).join(' union all ');
    };

    var mk_user_view_measure_queries = function(measure_conditions) {
      return mk_view_measure_queries(measure_conditions, function(err, cumulative, uniq_attr, attr_id, cb) {
        if (err) { cb(err); } else {
          cb(null, 'select ' + attr_id + ' as msid, udbid, ' + uniq_attr + ' as msum, ' + uniq_attr + '_count as mcnt from user_view');
        }
      }).join(' union all ');
    };


    ////////////////////////////////////////////////////
    // Unified population and measure queries
    // based on distributive partial aggregation.

    var mk_const_measure_attrs = function(aggregate, const_spec) {
      var uniq_attr = attrNameOfFilterSpec(const_spec);
      if ( aggregate === 'avg' ) {
        return [const_spec.val + ' as ' + uniq_attr, '0 as ' + uniq_attr + '_count'];
      } else {
        return [const_spec.val + ' as ' + uniq_attr];
      }
    };

    var mk_const_measure_query = function(aggregate, const_attrs) {
      var select_list = flattenArray(Object.keys(const_attrs).sort().map(function(k) {
        return const_attrs[k];
      })).join(', ');

      if ( aggregate === 'avg' ) {
        return '( select null::integer as userid, ' + select_list + ' )';
      } else {
        return '( select null::integer as userid, ' + select_list + ' )';
      }
    };

    var mk_popmeasure_query = function(tag, with_debug, aggregate, filter_conditions, measure_conditions, condition_xform, query_gen) {
      var new_conditions = map_all_conditions(filter_conditions.concat(measure_conditions),
        condition_xform,
        function(err, map_result) {
          if (err) { query_construction_errors.push(err); return []; }
          return map_result;
        }
      );

      // Aggregate expressions per user and day.
      var agg_select_list = {};

      // Aggregate expressions per user.
      var intermediate_select_list = {};

      // Non-aggregated expressions.
      var expr_select_list = {};

      new_conditions.forEach(function(cond_spec) {
        var uniq_attr = attrNameOfFilterSpec(cond_spec);
        if ( !( agg_select_list.hasOwnProperty(uniq_attr)
              || intermediate_select_list.hasOwnProperty(uniq_attr)
              || expr_select_list.hasOwnProperty(uniq_attr)
              ) )
        {
          var agg = aggregate === 'avg' ? 'sum' : aggregate;
          var exprs      = [mk_condition_expr(false, cond_spec) + ' as ' + uniq_attr];
          var agg_exprs  = [mk_condition_aggregator(agg, cond_spec) + ' as ' + uniq_attr];
          var intm_exprs = [agg + '(' + uniq_attr + ') as ' + uniq_attr];

          if ( aggregate === 'avg' ) {
            exprs.push(mk_condition_expr(true, cond_spec) + ' as ' + uniq_attr + '_count');
            agg_exprs.push(mk_condition_aggregator('count', cond_spec) + ' as ' + uniq_attr + '_count');
            if ( cond_spec.cumulative ) {
              intm_exprs.push(agg + '(case when ' + uniq_attr + '_count > 0 then 1 else 0 end) as ' + uniq_attr + '_count');
            } else {
              intm_exprs.push(agg + '(' + uniq_attr + '_count) as ' + uniq_attr + '_count');
            }
          }

          expr_select_list[uniq_attr] = exprs.join(', ');
          agg_select_list[uniq_attr] = agg_exprs.join(', ');
          intermediate_select_list[uniq_attr] = intm_exprs.join(', ');
        }
      });

      expr_select_list = Object.keys(expr_select_list).sort().map(function(k) { return expr_select_list[k]; }).join(', ');
      agg_select_list = Object.keys(agg_select_list).sort().map(function(k) { return agg_select_list[k]; }).join(', ');
      intermediate_select_list = Object.keys(intermediate_select_list).sort().map(function(k) { return intermediate_select_list[k]; }).join(', ');

      var where_clause = uniqueArray(new_conditions.filter(function(cond_spec) {
        return !strIsEmpty(cond_spec.row_condition);
      }).map(function(cond_spec) { return cond_spec.row_condition; })).join(' or ');

      return query_gen(expr_select_list, agg_select_list, intermediate_select_list, where_clause);
    };


    // Single-pass Granola population and measure query generator.
    // TODO: optimize redundant case conditions for singleton where clauses.
    var mk_granola_popmeasure_query = function(aggregate, filter_conditions, measure_conditions) {
      var query_gen = function(expr_select_list, agg_select_list, intermediate_select_list, where_clause) {
        var id_subquery = 'select max(last_measure_id) from measures_etl_progress where dataset_type = 0';

        var date_time_expr = 'body#>>\'{body, effective_time_frame, date_time}\'';
        var interval_start_expr = 'body#>>\'{body, effective_time_frame, time_interval, start_date_time}\'';
        var day_expr = 'date_trunc(\'day\', (coalesce(' + date_time_expr + ', ' + interval_start_expr + '))::timestamp)';

        var select_list_extra = 'body#>>\'{userid}\' as userid, ' + day_expr + ' as day';
        var where_extra = with_explicit_population ? ('body#>>\'{userid}\' in ( ' + population_userids.join(', ') + ' )') : '';

        var inner_query =
          'select ' + select_list_extra + ', ' + agg_select_list
            + ' from mc_granola_measures'
            + ' where ' + granola_timestamp_attr + ' between $1 and $2'
            + ' and id > (' + id_subquery + ')'
            + (strIsEmpty(where_extra) ? '' : (' and ' + where_extra))
            + (strIsEmpty(where_clause) ? '' : (' and ( ' + where_clause + ' )'))
            + ' group by body#>>\'{userid}\', ' + day_expr;

        if ( view_initializer ) {
          return inner_query;
        } else {
          return 'select userid, ' + intermediate_select_list + ' from (' + inner_query + ') R group by userid';
        }
      };

      return mk_popmeasure_query('GRN', false, aggregate, filter_conditions, measure_conditions, mk_granola_condition_exprs, query_gen);
    };


    // Single-pass MC-JSON population and measure query generator.
    // TODO: optimize redundant case conditions for singleton where clauses.
    var mk_mc_json_popmeasure_query = function(aggregate, json_row_schema, filter_conditions, measure_conditions) {
      var query_gen = function(expr_select_list, agg_select_list, intermediate_select_list, where_clause) {
        var day_expr = 'date_trunc(\'day\', r2.ts)';
        var id_subquery = 'select max(last_measure_id) from measures_etl_progress where dataset_type = 1';
        var select_list_extra = 'userid, ' + day_expr + ' as day';
        var where_extra = with_explicit_population ? ('userid in ( ' + population_userids.join(', ') + ' )') : '';

        var inner_query =
          ' select ' + select_list_extra + ', ' + agg_select_list
            + ' from (select userid, body from mc_json_measures where id > (' + id_subquery + ')) r,'
            + ' lateral json_to_record(r.body::json) as r2(ts timestamp, ' + json_row_schema + ')'
            + ' where extract(epoch from r2.ts) between $1 and $2'
            + (strIsEmpty(where_extra) ? '' : (' and ' + where_extra))
            + (strIsEmpty(where_clause) ? '' : (' and ( ' + where_clause + ' )'))
            + ' group by userid, ' + day_expr;

        if ( view_initializer ) {
          return inner_query;
        } else {
          return 'select userid, ' + intermediate_select_list + ' from (' + inner_query + ') R group by userid';
        }
      };

      return mk_popmeasure_query('MCJSON', false, aggregate, filter_conditions, measure_conditions, mk_mc_json_condition_exprs, query_gen);
    };

    // Single-pass MC population and measure query generator.
    var mk_mc_relation_popmeasure_query = function(aggregate, relation, filter_conditions, measure_conditions, all_attrs) {
      var query_gen = function(expr_select_list, agg_select_list, intermediate_select_list, where_clause) {
        var day_expr = 'date_trunc(\'day\', ts)';
        var where_extra = with_explicit_population ? ('U.id in ( ' + population_userids.join(', ') + ' )') : '';

        var inner_query =
          'select encode(U.id, \'base64\') as userid, ' + day_expr + ' as day, ' + agg_select_list
            + ' from ' + relation + ' R, users U'
            + ' where extract(epoch from ts) between $1 and $2'
            + (strIsEmpty(where_extra) ? '' : (' and ' + where_extra))
            + (strIsEmpty(where_clause) ? '' : (' and ( ' + where_clause + ' )'))
            + ' and R.udbid = U.udbid'
            + ' group by U.id, ' + day_expr;

        if ( view_initializer ) {
          return '( ' + inner_query + ' )';
        } else {
          return '( select userid, ' + intermediate_select_list + ' from ( ' + inner_query + ' ) R group by userid)';
        }
      }

      return mk_popmeasure_query('MC', false, aggregate, filter_conditions, measure_conditions, mk_mc_condition_exprs, query_gen);
    };

    // NOTE: we should never use NHANES data for view intialization queries.
    var mk_nhanes_relation_popmeasure_query = function(aggregate, relation, filter_conditions, measure_conditions, all_attrs) {
      var new_filter_conditions = map_all_conditions(filter_conditions,
        function(cond_spec, cb) { mk_nhanes_condition_exprs(cond_spec, cb); },
        function(err, map_result) {
          if (err) { query_construction_errors.push(err); return []; }
          return map_result;
        }
      );

      var population_where_clause = new_filter_conditions.map(function(cond_spec) {
        var filtered_expr = mk_condition_expr(false, cond_spec);
        if ( filtered_expr && cond_spec.having_condition ) {
          return filtered_expr + ' ' + cond_spec.having_condition;
        } else {
          return '';
        }
      }).filter(function(s) { return !strIsEmpty(s); }).join(' and ');

      var query_gen = function(expr_select_list, agg_select_list, intermediate_select_list, where_clause) {
        var combined_where_clause = [where_clause, population_where_clause].filter(function(s) {
          return !strIsEmpty(s);
        }).join(' and ');

        return '( select unique_id as userid, ' + expr_select_list
                  + ' from ' + relation
                  + (strIsEmpty(combined_where_clause) ? '' : (' where ( ' + combined_where_clause + ' )'))
                  + ')';
      };

      return mk_popmeasure_query('NHANES', false, aggregate, filter_conditions, measure_conditions, mk_nhanes_condition_exprs, query_gen);
    };

    var mk_popmeasure_query_by_relations = function(tag, aggregate, integer_userids, unique_userids,
                                                    const_specs, tbl_filter_specs, tbl_measure_specs, condition_xform,
                                                    const_attrgen, const_querygen, relation_querygen)
    {
      var const_attrs = [];
      var const_subquery_attrs = {};

      // Top-level aggregation expressions, over all users
      var join_select_list_by_attrs = {};

      var const_conditions = map_all_conditions(const_specs,
        function(cond_spec, cb) { condition_xform(cond_spec, cb); },
        function(err, map_result) {
          if (err) { query_construction_errors.push(err); return []; }
          return map_result;
        }
      );

      const_conditions.forEach(function(spec) {
        var uniq_attr = attrNameOfFilterSpec(spec);
        var agg = aggregate === 'avg' ? 'sum' : aggregate;

        const_attrs.push(uniq_attr);
        if ( !const_subquery_attrs.hasOwnProperty(uniq_attr) ) {
          const_subquery_attrs[uniq_attr] = const_attrgen(aggregate, extendObject({val: '0.0'}, spec));
        }

        var exprs = [];
        if ( unique_userids ) {
          exprs = ['(case when ' + uniq_attr + ' is null then 0 else ' + uniq_attr + ' end) as ' + uniq_attr];
          if ( aggregate === 'avg' ) {
            exprs.push('(case when ' + uniq_attr + '_count is null then 0 else ' + uniq_attr + '_count end) as ' + uniq_attr + '_count');
          }
        } else {
          var agg = aggregate === 'avg' ? 'sum' : aggregate;
          exprs = [agg + '(case when ' + uniq_attr + ' is null then 0 else ' + uniq_attr + ' end) as ' + uniq_attr];
          if ( aggregate === 'avg' ) {
            exprs.push(agg + '(case when ' + uniq_attr + '_count is null then 0 else ' + uniq_attr + '_count end) as ' + uniq_attr + '_count');
          }
        }
        join_select_list_by_attrs[uniq_attr] = exprs;
      });

      var const_subquery = objIsEmpty(const_subquery_attrs) ? '' : (const_querygen(aggregate, const_subquery_attrs));
      var const_subqueries = strIsEmpty(const_subquery) ? [] : [const_subquery];

      var naive_clone = function(o) { return JSON.parse(JSON.stringify(o)); }

      var all_specs = naive_clone(tbl_filter_specs);
      Object.keys(tbl_measure_specs).forEach(function(relation) {
        all_specs[relation] = (all_specs.hasOwnProperty(relation) ? all_specs[relation] : []).concat(naive_clone(tbl_measure_specs[relation]));
      });

      // Force a copy since mk_mc_all_attrs modifies its arguments.
      var cloned_specs = naive_clone(all_specs);
      var all_attrs = uniqueArray(const_attrs.concat(mk_mc_all_attrs(cloned_specs))).sort();
      var all_relations = Object.keys(all_specs);

      var join_subqueries = all_relations.map(function(relation) {
        var filter_conditions = tbl_filter_specs.hasOwnProperty(relation) ? naive_clone(tbl_filter_specs[relation]) : [];
        var measure_conditions = tbl_measure_specs.hasOwnProperty(relation) ? naive_clone(tbl_measure_specs[relation]) : [];
        return relation_querygen(aggregate, relation, filter_conditions, measure_conditions, all_attrs);
      });

      var join_attrs = view_initializer ? 'userid, day' : 'userid';

      var join_pair = function(force_outer, lquery, rquery, join_attrs) {
        if ( strIsEmpty(rquery) ) { return lquery; }
        else if ( strIsEmpty(lquery) ) { return rquery; }
        else if ( force_outer || objIsEmpty(tbl_filter_specs) ) {
          return lquery + ' full outer join ' + rquery + ' using (' + join_attrs + ')';
        }
        else {
          return lquery + ' join ' + rquery + ' using (' + join_attrs + ')';
        }
      };

      var join_from = '';
      var join_chain = join_subqueries.map(function(subquery, idx) { return subquery + ' R' + idx; });

      if ( arrIsEmpty(join_chain) && !strIsEmpty(const_subquery) ) {
        // If we only have constants, we don't need any kind of joins.
        join_from = const_subquery + ' R0';
      }
      else {
        join_from = join_chain.reduce(function(acc, sq) {
          return join_pair(false, acc, sq, join_attrs);
        }, join_from);

        var const_subquery_alias = strIsEmpty(const_subquery) ?
          const_subquery : ( const_subquery + ' R' + (join_subqueries.length) );

        join_from = join_pair(true, join_from, const_subquery_alias, join_attrs);
      }

      all_relations.forEach(function(relation) {
        var conditions = [];
        if ( tbl_filter_specs.hasOwnProperty(relation) ) { conditions = conditions.concat(tbl_filter_specs[relation]); }
        if ( tbl_measure_specs.hasOwnProperty(relation) ) { conditions = conditions.concat(tbl_measure_specs[relation]); }

        var new_conditions = map_all_conditions(conditions,
          function(cond_spec, cb) { mk_nhanes_condition_exprs(cond_spec, cb); },
          function(err, map_result) {
            if (err) { query_construction_errors.push(err); return []; }
            return map_result;
          }
        );

        new_conditions.forEach(function(cond_spec) {
          var uniq_attr = attrNameOfFilterSpec(cond_spec);
          var agg = aggregate === 'avg' ? 'sum' : aggregate;
          var exprs = [];
          if ( unique_userids ) {
            exprs = ['(case when ' + uniq_attr + ' is null then 0 else ' + uniq_attr + ' end) as ' + uniq_attr];
            if ( aggregate === 'avg' ) {
              exprs.push('(case when ' + uniq_attr + '_count is null then 0 else ' + uniq_attr + '_count end) as ' + uniq_attr + '_count');
            }
          } else {
            var agg = aggregate === 'avg' ? 'sum' : aggregate;
            exprs = [agg + '(case when ' + uniq_attr + ' is null then 0 else ' + uniq_attr + ' end) as ' + uniq_attr];
            if ( aggregate === 'avg' ) {
              exprs.push(agg + '(case when ' + uniq_attr + '_count is null then 0 else ' + uniq_attr + '_count end) as ' + uniq_attr + '_count');
            }
          }
          join_select_list_by_attrs[uniq_attr] = exprs;
        });
      });

      var join_select_list = all_attrs.map(function(attr) {
        if ( join_select_list_by_attrs.hasOwnProperty(attr) ) {
          return join_select_list_by_attrs[attr].join(', ')
        } else {
          return aggregate === 'avg' ? 'null as ' + attr + ', null as ' + attr + '_count' : 'null as ' + attr;
        }
      }).join(', ');

      var userid_exprs = join_subqueries.concat(const_subqueries).map(function(subquery, idx) { return 'R' + idx + '.userid'; });
      var userid_expr = 'coalesce(' + userid_exprs.join(', ') + ', ' + (integer_userids ? '0' : '\'DEFAULTUSER\'') + ')';
      var userid_select = userid_expr + (integer_userids ? '::text' : '');

      if ( view_initializer ) {
        var day_exprs = join_subqueries.map(function(subquery, idx) { return 'R' + idx + '.day'; });
        var day_expr = 'coalesce(' + day_exprs.join(', ') + ')';
        return 'select ' + userid_select + ' as userid, ' + day_expr + ' as day, ' + join_select_list
                  + ' from ' + join_from + ' group by ' + userid_expr + ', ' + day_expr;
      } else {
        var with_fastpath = false;
        var fastpath = with_fastpath ? (' order by ' + userid_expr + ' limit 10000') : '';

        return 'select ' + userid_select + ' as userid, ' + join_select_list
                  + ' from ' + join_from + (unique_userids ? fastpath : (' group by ' + userid_expr));
      }
    };

    var mk_nhanes_measure_query = function(aggregate, const_specs, tbl_filter_specs, tbl_measure_specs) {
      return mk_popmeasure_query_by_relations('NHANES', aggregate, true, true, const_specs, tbl_filter_specs, tbl_measure_specs,
               mk_nhanes_condition_exprs, mk_const_measure_attrs, mk_const_measure_query, mk_nhanes_relation_popmeasure_query);
    };

    var mk_mc_popmeasure_query = function(aggregate, tbl_filter_specs, tbl_measure_specs) {
      return mk_popmeasure_query_by_relations('MC', aggregate, false, false, [], tbl_filter_specs, tbl_measure_specs,
               mk_mc_condition_exprs, mk_const_measure_attrs, mk_const_measure_query, mk_mc_relation_popmeasure_query);
    };

    // Array of subqueries for final aggregation.
    var query_sources = [];

    if ( use_nhanes ) {
      var nhanes_query = mk_nhanes_measure_query(aggregate, nhanes_constant_queries, nhanes_conjuncts, nhanes_queries);

      if ( !view_initializer ) {
        // plv8.elog(WARNING, 'NHANES query: ', nhanes_query);
        query_sources.push(nhanes_query);
      }
    }

    if ( use_granola ) {
      var granola_query = mk_granola_popmeasure_query(aggregate, granola_conjuncts, granola_queries);
      // plv8.elog(WARNING, 'MC-Granola query: ', granola_query);
      query_sources.push(granola_query);
    }

    if ( use_mc_json ) {
      var mc_json_query = mk_mc_json_popmeasure_query(aggregate, mc_json_row_schema, mc_json_conjuncts, mc_json_queries);
      // plv8.elog(WARNING, 'MC-JSON query: ', mc_json_query);
      query_sources.push(mc_json_query);
    }

    if ( use_mc ) {
      var mc_query = mk_mc_popmeasure_query(aggregate, mc_conjuncts, mc_queries);
      // plv8.elog(WARNING, 'MC-Native query: ', mc_query);
      query_sources.push(mc_query);
    }

    if ( use_mc_view ) {
      var with_user_day_view = false;

      var user_view_measure_ids = [];
      var user_view_select_list_by_attrs = {};

      var user_day_view_measure_ids = [];
      var user_day_view_select_list_by_attrs = {};

      var view_select_list_by_attrs = {};

      var combined_specs = map_all_conditions(granola_conjuncts.concat(granola_queries),
        mk_granola_condition_exprs, function(err, map_result) {
          if (err) { query_construction_errors.push(err); return []; }
          return map_result;
        }
      );

      combined_specs.forEach(function (col_spec) {
        var attr_id = attrIdOfFilterSpec(col_spec).toString();
        var uniq_attr = attrNameOfFilterSpec(col_spec);
        if ( with_user_day_view && col_spec.cumulative ) {
          if ( !user_day_view_select_list_by_attrs.hasOwnProperty(uniq_attr) ) {
            user_day_view_select_list_by_attrs[uniq_attr] = [
              ('sum(case when msid = ' + attr_id + ' then msum else 0 end) as ' + uniq_attr),
              ('sum(case when msid = ' + attr_id + ' and mcnt > 0 then 1 else 0 end) as ' + uniq_attr + '_count')
            ];
            user_day_view_measure_ids.push(attr_id);
          }
        } else {
          if ( !user_view_select_list_by_attrs.hasOwnProperty(uniq_attr) ) {
            user_view_select_list_by_attrs[uniq_attr] = [
              ('sum(case when msid = ' + attr_id + ' then msum else 0 end) as ' + uniq_attr),
              ('sum(case when msid = ' + attr_id + ' then mcnt else 0 end) as ' + uniq_attr + '_count')
            ];
            user_view_measure_ids.push(attr_id);
          }
        }

        if ( !view_select_list_by_attrs.hasOwnProperty(uniq_attr) ) {
          view_select_list_by_attrs[uniq_attr] = [
            ('sum(case when ' + uniq_attr + ' is null then 0 else ' + uniq_attr + ' end) as ' + uniq_attr),
            ('sum(case when ' + uniq_attr + '_count is null then 0 else ' + uniq_attr + '_count end) as ' + uniq_attr + '_count')
          ];
        }
      });

      var mc_user_view_select_list = Object.keys(user_view_select_list_by_attrs).sort().map(function(attr) {
        return user_view_select_list_by_attrs[attr].join(', ');
      }).join(', ');

      var mc_user_day_view_select_list = Object.keys(user_day_view_select_list_by_attrs).sort().map(function(attr) {
        return user_day_view_select_list_by_attrs[attr].join(', ');
      }).join(', ');

      var mc_view_select_list = Object.keys(view_select_list_by_attrs).sort().map(function(attr) {
        return view_select_list_by_attrs[attr].join(', ');
      }).join(', ');

      var mc_user_view_query = 'select encode(U.id, \'base64\') as userid, ' + mc_user_view_select_list
                                 + ' from mc_sumcount_by_user R, users U'
                                 + ' where msid in ( ' + user_view_measure_ids.join(', ') + ' )'
                                 + ' and   R.udbid = U.udbid'
                                 + ' group by U.id';

      var mc_user_day_view_query = 'select encode(U.id, \'base64\') as userid, ' + mc_user_day_view_select_list
                                     + 'from mc_sumcount_by_user_day R, users U'
                                     + 'where msid in ( ' + user_day_view_measure_ids.join(', ') + ' )'
                                     + 'and   R.udbid = U.udbid'
                                     + 'group by U.id';

      var mc_view_query = '';

      if ( !(objIsEmpty(user_view_select_list_by_attrs) && objIsEmpty(user_day_view_select_list_by_attrs)) ) {
        if ( objIsEmpty(user_view_select_list_by_attrs) ) {
          mc_view_query = mc_user_day_view_query;
        } else if ( objIsEmpty(user_day_view_select_list_by_attrs) ) {
          mc_view_query = mc_user_view_query;
        } else {
          mc_view_query = 'select coalesce(V0.userid, V1.userid) as userid, ' + mc_view_select_list
                            + ' from ( ' + mc_user_view_query + ' ) V0 full outer join ( ' + mc_user_day_view_query + ' ) V1 using (userid)'
                            + ' group by coalesce(V0.userid, V1.userid)';
        }

        // plv8.elog(WARNING, 'Pushed MC-View query', mc_view_query);
        query_sources.push(mc_view_query);
      }
    }

    // Finalization.
    if ( query_sources.length > 1 ) {
      query_sources = query_sources.map(function (q) { return '(' + q + ')'; });
    }

    var query = '';

    if ( view_initializer ) {
      var new_conditions = map_all_conditions(granola_conjuncts.concat(granola_queries),
        mk_granola_condition_exprs, function(err, map_result) {
          if (err) { query_construction_errors.push(err); return []; }
          return map_result;
        }
      );

      // Aggregate expressions per user.
      var intermediate_select_list = {};

      new_conditions.forEach(function(cond_spec) {
        var uniq_attr = attrNameOfFilterSpec(cond_spec);
        if ( !intermediate_select_list.hasOwnProperty(uniq_attr) ) {
          var agg = aggregate === 'avg' ? 'sum' : aggregate;
          var intm_exprs = [agg + '(' + uniq_attr + ') as ' + uniq_attr];
          if ( aggregate === 'avg' ) {
            if ( cond_spec.cumulative ) {
              intm_exprs.push(agg + '(case when ' + uniq_attr + '_count > 0 then 1 else 0 end) as ' + uniq_attr + '_count');
            } else {
              intm_exprs.push(agg + '(' + uniq_attr + '_count) as ' + uniq_attr + '_count');
            }
          }
          intermediate_select_list[uniq_attr] = intm_exprs.join(', ');
        }
      });

      intermediate_select_list = Object.keys(intermediate_select_list).map(function(k) { return intermediate_select_list[k]; }).sort().join(', ');

      // Note that we do not return a query string for a view initializer.
      var user_day_view            = 'select U.udbid, VI.* from ( ' + query_sources.join(' union all ') + ' ) VI, users U where decode(VI.userid, \'base64\') = U.id';
      var user_view                = 'select udbid, ' + intermediate_select_list + ' from user_day_view R group by udbid';
      var user_day_measure_queries = mk_user_day_view_measure_queries(granola_queries);
      var user_measure_queries     = mk_user_view_measure_queries(granola_queries);

      var user_day_view_updates = '';
      var user_day_view_update_count = '0';

      if ( !strIsEmpty(user_day_measure_queries) ) {
        user_day_view_updates =
              ', by_user_day_view_updates as ('
           +  '   insert into mc_sumcount_by_user_day as original(msid, udbid, day, msum, mcnt)'
           +  '     select VI.msid, VI.udbid, VI.day, coalesce(sum(VI.msum), 0) as msum, coalesce(sum(VI.mcnt), 0) as mcnt'
           +  '     from ( ' + user_day_measure_queries + ' ) VI'
           +  '     group by VI.msid, VI.udbid, VI.day'
           +  '   on conflict(msid, udbid, day) do update'
           +  '     set msum = excluded.msum, mcnt = excluded.mcnt'
           +  '     where original.msid  = excluded.msid'
           +  '     and   original.udbid = excluded.udbid'
           +  '     and   original.day   = excluded.day'
           +  '   returning msid, udbid, day'
           +  ' )'

        user_day_view_update_count = 'select count(*) from by_user_day_view_updates';
      }

      var user_view_updates = '';
      var user_view_update_count = '0';

      if ( !strIsEmpty(user_measure_queries) ) {
        user_view_updates =
              ', by_user_view_updates as ('
           +  '   insert into mc_sumcount_by_user as original(msid, udbid, msum, mcnt)'
           +  '     select VI.msid, VI.udbid, coalesce(sum(VI.msum), 0) as msum, coalesce(sum(VI.mcnt), 0) as mcnt'
           +  '     from ( ' + user_measure_queries + ' ) VI'
           +  '     group by VI.msid, VI.udbid'
           +  '   on conflict(msid, udbid) do update'
           +  '     set msum = excluded.msum, mcnt = excluded.mcnt'
           +  '     where original.msid  = excluded.msid'
           +  '     and   original.udbid = excluded.udbid'
           +  '   returning msid, udbid'
           +  ' )';

        user_view_update_count = 'select count(*) from by_user_view_updates';
      }

      query =
        'with user_day_view as ( ' + user_day_view + ' ),'
         +  ' user_view as ( ' + user_view + ' )'
         +  user_day_view_updates
         +  user_view_updates
         +  ' select (' + user_day_view_update_count + ') as by_user_day_updates,'
         +  '        (' + user_view_update_count + ') as by_user_updates'
         ;

    } else {
      var query = '';
      var mc_intermediate_aggs = [];
      var mc_final_aggs = [];

      column_specs.forEach(function(col_spec) {
        var uniq_attr = attrNameOfFilterSpec(col_spec);
        if ( aggregate === 'avg' ) {
          mc_intermediate_aggs.push('sum(' + uniq_attr + ') as ' + uniq_attr);
          mc_intermediate_aggs.push('sum(' + uniq_attr + '_count) as ' + uniq_attr + '_count');
          mc_final_aggs.push('(sum(' + uniq_attr + ') / greatest(sum(' + uniq_attr + '_count), 1)) as ' + uniq_attr);
        } else {
          mc_intermediate_aggs.push(aggregate + '(' + uniq_attr + ') as ' + uniq_attr);
          mc_final_aggs.push(aggregate + '(' + uniq_attr + ') as ' + uniq_attr);
        }
      });

      var mc_having_clause = Object.keys(mc_conjuncts).map(function(relation) {
        var conditions = mc_conjuncts[relation];
        var new_conditions = map_all_conditions(conditions,
          function(cond_spec, cb) { mk_mc_condition_exprs(cond_spec, cb); },
          function(err, map_result) {
            if (err) { query_construction_errors.push(err); return []; }
            return map_result;
          }
        );

        return new_conditions.map(function(cond_spec){
          if ( !strIsEmpty(cond_spec.having_condition) ) {
            var uniq_attr = attrNameOfFilterSpec(cond_spec);
            var outer_agg_expr = aggregate === 'avg' ?
              ('sum(' + uniq_attr + ') / greatest(1, sum(' + uniq_attr + '_count))') : aggregate + '(' + uniq_attr + ')';
            return outer_agg_expr + ' ' + cond_spec.having_condition;
          }
        }).filter(function(s) { return !strIsEmpty(s); }).join(' and ');
      }).filter(function(s) { return !strIsEmpty(s); }).join(' and ');

      if ( strIsEmpty(mc_having_clause) ) {
        query = 'select ' + mc_final_aggs.sort().join(', ') + ' from ('
                + query_sources.join(' union all ')
                + ' ) NHMC';
      } else {
        query = 'select ' + mc_final_aggs.sort().join(', ') + ' from ('
                + ' select userid, ' + mc_intermediate_aggs.sort().join(', ')
                + ' from ( ' + query_sources.join(' union all ') + ' ) R'
                + ' group by userid ' + (strIsEmpty(mc_having_clause) ? '' : (' having ' + mc_having_clause)) + ' ) NHMC';
      }
    }

    // Report errors in building population queries.
    if ( !arrIsEmpty(query_construction_errors) ) {
      plv8.elog(ERROR, 'Failed to build population queries:\n' + query_construction_errors.join('\n'));
      return null;
    }

    // plv8.elog(WARNING, query);
    return plv8.execute(query, with_parameters ? [ tstart, tend ] : []);
  }
  catch (e) {
    return plv8.elog(ERROR, e.message);
    return null;
  }
$$
language plv8;
