#!/usr/bin/env ruby
## TODO: HKQuantityTypeIdentifierDistanceWalkingRunning and HKQuantityTypeIdentifierFlightsClimbed are not activities,
# but we want to store them in our table as such. Revise their extractors.

distkcal_fields = ['distance', 'kcal_burned'].map {|n| ["'#{n}'", "(body#>'{body, #{n}}')::text"]}.flatten().join(', ')
activity_fields = [['activity_type', 'activity' ], ['activity_duration', 'duration'], ['activity_value' , 'distkcal']]

interval_endpoints = ['end_date_time', 'start_date_time'].map { |i| "(body#>'{body, effective_time_frame, time_interval, #{i}}')::text::timestamptz"}
time_interval = "case when body#>'{body, effective_time_frame}' ? 'date_time' then 0.0 " +
                "else (extract(epoch from (#{interval_endpoints[0]} - #{interval_endpoints[1]})))::real end"

$dataset_types = {
  'granola' => 0,
  'mc_json' => 1
}

$dbschema = {
  'body_weight'                         => ['real'    , 'mc_body_measures'               ] ,
  'body_height'                         => ['real'    , 'mc_body_measures'               ] ,
  'body_mass_index'                     => ['real'    , 'mc_body_measures'               ] ,
  'body_fat_percentage'                 => ['real'    , 'mc_body_measures'               ] ,
  'body_temperature'                    => ['real'    , 'mc_body_measures'               ] ,
  'basal_body_temperature'              => ['real'    , 'mc_body_measures'               ] ,
  'lean_body_mass'                      => ['real'    , 'mc_body_measures'               ] ,
  'systolic_blood_pressure'             => ['real'    , 'mc_blood_pressure_measures'     ] ,
  'diastolic_blood_pressure'            => ['real'    , 'mc_blood_pressure_measures'     ] ,
  'sleep_duration'                      => ['real'    , 'mc_sleep_measures'              ] ,
  'meal_duration'                       => ['real'    , 'mc_meal_measures'               ] ,
  'food_type'                           => ['jsonb'   , 'mc_meal_measures'               ] ,
  'activity_duration'                   => ['real'    , 'mc_activity_measures'           ] ,
  'activity_type'                       => ['integer' , 'mc_activity_measures'           ] ,
  'activity_value'                      => ['jsonb'   , 'mc_activity_measures'           ] ,
  'uv_exposure'                         => ['real'    , 'mc_light_measures'              ] ,
  'active_energy_burned'                => ['real'    , 'mc_energy_measures'             ] ,
  'basal_energy_burned'                 => ['real'    , 'mc_energy_measures'             ] ,
  'blood_alcohol_content'               => ['real'    , 'mc_blood_measures'              ] ,
  'blood_glucose'                       => ['real'    , 'mc_blood_measures'              ] ,
  'blood_oxygen_saturation'             => ['real'    , 'mc_blood_measures'              ] ,
  'forced_expiratory_volume_one_second' => ['real'    , 'mc_lung_measures'               ] ,
  'forced_vital_capacity'               => ['real'    , 'mc_lung_measures'               ] ,
  'peak_expiratory_flow'                => ['real'    , 'mc_lung_measures'               ] ,
  'inhaler_usage'                       => ['real'    , 'mc_lung_measures'               ] ,
  'respiratory_rate'                    => ['real'    , 'mc_lung_measures'               ] ,
  'heart_rate'                          => ['real'    , 'mc_heart_rate_measures'         ] ,
  'dietary_carbohydrates'               => ['real'    , 'mc_nutrients_macro_measures'    ] ,
  'dietary_energy_consumed'             => ['real'    , 'mc_nutrients_macro_measures'    ] ,
  'dietary_fat_total'                   => ['real'    , 'mc_nutrients_macro_measures'    ] ,
  'dietary_protein'                     => ['real'    , 'mc_nutrients_macro_measures'    ] ,
  'dietary_caffeine'                    => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_cholesterol'                 => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_fat_monounsaturated'         => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_fat_polyunsaturated'         => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_fat_saturated'               => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_fiber'                       => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_sugar'                       => ['real'    , 'mc_nutrients_subsets_measures'  ] ,
  'dietary_calcium'                     => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_chloride'                    => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_chromium'                    => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_copper'                      => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_iodine'                      => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_iron'                        => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_magnesium'                   => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_manganese'                   => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_molybdenum'                  => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_niacin'                      => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_phosphorus'                  => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_potassium'                   => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_selenium'                    => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_sodium'                      => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_zinc'                        => ['real'    , 'mc_nutrients_minerals_measures' ] ,
  'dietary_biotin'                      => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_folate'                      => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_pantothenic_acid'            => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_riboflavin'                  => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_thiamin'                     => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitamina'                    => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitaminb12'                  => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitaminb6'                   => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitaminc'                    => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitamind'                    => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitamine'                    => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_vitamink'                    => ['real'    , 'mc_nutrients_vitamins_measures' ] ,
  'dietary_alcohol'                     => ['real'    , 'mc_nutrients_liquids_measures'  ] ,
  'dietary_water'                       => ['real'    , 'mc_nutrients_liquids_measures'  ] ,
  'apple_stand_hour'                    => ['real'    , 'mc_misc_measures'               ] ,
  'electrodermal_activity'              => ['real'    , 'mc_misc_measures'               ] ,
  'nike_fuel'                           => ['real'    , 'mc_misc_measures'               ] ,
  'number_of_times_fallen'              => ['real'    , 'mc_misc_measures'               ] ,
  'peripheral_perfusion_index'          => ['real'    , 'mc_misc_measures'               ]
}

$dbschema_by_rel = Hash[$dbschema.group_by { |k,v| v[1] }.map { |rel, attrs_types_rels| [rel, attrs_types_rels.map { |a,tr| [a, tr[0]] }] }]

$max_fields = 6

$activity_types = {
    'HKWorkoutActivityTypeAmericanFootball'             => 1,
    'HKWorkoutActivityTypeArchery'                      => 2,
    'HKWorkoutActivityTypeAustralianFootball'           => 3,
    'HKWorkoutActivityTypeBadminton'                    => 4,
    'HKWorkoutActivityTypeBaseball'                     => 5,
    'HKWorkoutActivityTypeBasketball'                   => 6,
    'HKWorkoutActivityTypeBowling'                      => 7,
    'HKWorkoutActivityTypeBoxing'                       => 8,
    'HKWorkoutActivityTypeClimbing'                     => 9,
    'HKWorkoutActivityTypeCricket'                      => 10,
    'HKWorkoutActivityTypeCrossTraining'                => 11,
    'HKWorkoutActivityTypeCurling'                      => 12,
    'HKWorkoutActivityTypeCycling'                      => 13,
    'HKWorkoutActivityTypeDance'                        => 14,
    'HKWorkoutActivityTypeDanceInspiredTraining'        => 15,
    'HKWorkoutActivityTypeElliptical'                   => 16,
    'HKWorkoutActivityTypeEquestrianSports'             => 17,
    'HKWorkoutActivityTypeFencing'                      => 18,
    'HKWorkoutActivityTypeFishing'                      => 19,
    'HKWorkoutActivityTypeFunctionalStrengthTraining'   => 20,
    'HKWorkoutActivityTypeGolf'                         => 21,
    'HKWorkoutActivityTypeGymnastics'                   => 22,
    'HKWorkoutActivityTypeHandball'                     => 23,
    'HKWorkoutActivityTypeHiking'                       => 24,
    'HKWorkoutActivityTypeHockey'                       => 25,
    'HKWorkoutActivityTypeHunting'                      => 26,
    'HKWorkoutActivityTypeLacrosse'                     => 27,
    'HKWorkoutActivityTypeMartialArts'                  => 28,
    'HKWorkoutActivityTypeMindAndBody'                  => 29,
    'HKWorkoutActivityTypeMixedMetabolicCardioTraining' => 30,
    'HKWorkoutActivityTypePaddleSports'                 => 31,
    'HKWorkoutActivityTypePlay'                         => 32,
    'HKWorkoutActivityTypePreparationAndRecovery'       => 33,
    'HKWorkoutActivityTypeRacquetball'                  => 34,
    'HKWorkoutActivityTypeRowing'                       => 35,
    'HKWorkoutActivityTypeRugby'                        => 36,
    'HKWorkoutActivityTypeRunning'                      => 37,
    'HKWorkoutActivityTypeSailing'                      => 38,
    'HKWorkoutActivityTypeSkatingSports'                => 39,
    'HKWorkoutActivityTypeSnowSports'                   => 40,
    'HKWorkoutActivityTypeSoccer'                       => 41,
    'HKWorkoutActivityTypeSoftball'                     => 42,
    'HKWorkoutActivityTypeSquash'                       => 43,
    'HKWorkoutActivityTypeStairClimbing'                => 44,
    'HKWorkoutActivityTypeSurfingSports'                => 45,
    'HKWorkoutActivityTypeSwimming'                     => 46,
    'HKWorkoutActivityTypeTableTennis'                  => 47,
    'HKWorkoutActivityTypeTennis'                       => 48,
    'HKWorkoutActivityTypeTrackAndField'                => 49,
    'HKWorkoutActivityTypeTraditionalStrengthTraining'  => 50,
    'HKWorkoutActivityTypeVolleyball'                   => 51,
    'HKWorkoutActivityTypeWalking'                      => 52,
    'HKWorkoutActivityTypeWaterFitness'                 => 53,
    'HKWorkoutActivityTypeWaterPolo'                    => 54,
    'HKWorkoutActivityTypeWaterSports'                  => 55,
    'HKWorkoutActivityTypeWrestling'                    => 56,
    'HKWorkoutActivityTypeYoga'                         => 57,
    'HKWorkoutActivityTypeOther'                        => 3000,
    'step_count'                                        => 58,
    'HKQuantityTypeIdentifierDistanceWalkingRunning'    => 59,
    'HKQuantityTypeIdentifierFlightsClimbed'            => 60
  }

$name_mappings = {
    'HKQuantityTypeIdentifierDistanceWalkingRunning'    => 'distance',
    'HKQuantityTypeIdentifierFlightsClimbed'            => 'flights'
  }

$path_mappings = {
    'HKQuantityTypeIdentifierFlightsClimbed'            => 'count'
  }

$extractors = {
  #
  # Date/time extractors
    "date_time"    => ["body#>'{body, effective_time_frame, date_time}'",                      "text::timestamptz"],
    "start_time"   => ["body#>'{body, effective_time_frame, time_interval, start_date_time}'", "text::timestamptz"],
    "end_time"     => ["body#>'{body, effective_time_frame, time_interval, end_date_time}'",   "text::timestamptz"],
    "duration"     => ["body#>'{body, duration, value}'",                                      "text::real"],
    "duration_ts"  => ["to_json(#{time_interval})::jsonb",                                     "text::real"],
  #
  # Value extractors
    "unnamed_val"  => ["body#>'{body, unit_value, value}'",          "text::real"],
    "named_val"    => [lambda {|n| "body#>'{body, #{n}, value}'" },  "text::real"],
    "named"        => [lambda {|n| "body#>'{body, #{n}}'" },         "text::real"],
    "count"        => [lambda {|n| "body#>'{body, count}'" },        "text::real"],
  #
  # Enumeration extractors
    "activity"     => [lambda {|n| "to_json(#{$activity_types[n]})" },        "text::integer"],
  #
  # Metadata extractors
    "unnamedjson"  => [lambda {|n| "json_build_object('#{n}', (body#>'{body, unit_value, value}')::text::real)" },                    "jsonb"],
    "namedjson"    => [lambda {|n| "json_build_object('#{n}', (body#>'{body, #{n}}')::text::real)" },                                 "jsonb"],
    "renamedjson"  => [lambda {|n| "json_build_object('#{$name_mappings[n]}', (body#>'{body, unit_value, value}')::text::real)" },    "jsonb"],
    "mappedjson"   => [lambda {|n| "json_build_object('#{$name_mappings[n]}', (body#>'{body, #{$path_mappings[n]}}')::text::real)" }, "jsonb"],
    "jsonmetadata" => ["body#>'{body, metadata}'",                                                                                    "jsonb"],
    "distkcal"     => ["json_object(ARRAY[#{distkcal_fields}])",                                                                      "jsonb"],

    #
    # Constants
    "zero"         => ["to_json(0)",  "text::real"],
    "minus1"       => ["to_json(-1)", "text::real"],
  }

def extractor_for_category(k)
  return [lambda {|n,v| "case when body#>>'{category_value}' = E'#{v}' then #{$extractors[k][0]} else to_jsonb(0.0) end"}, $extractors[k][1]]
end

$category_extractors = {
  "duration_ts"  => extractor_for_category("duration_ts"),
}

# All HealthKit attributes except:
#   HKQuantityTypeIdentifierStepCount
#   HKWorkoutTypeIdentifier

unnamed = {
    'HKQuantityTypeIdentifierActiveEnergyBurned'        => ['mc_energy_measures',                           [['active_energy_burned'       , 'unnamed_val']]],
    'HKQuantityTypeIdentifierBasalEnergyBurned'         => ['mc_energy_measures',                           [['basal_energy_burned'        , 'unnamed_val']]],
    'HKQuantityTypeIdentifierBloodPressureDiastolic'    => ['mc_blood_pressure_measures',                   [['diastolic_blood_pressure'   , 'unnamed_val']]],
    'HKQuantityTypeIdentifierBloodPressureSystolic'     => ['mc_blood_pressure_measures',                   [['systolic_blood_pressure'    , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryAlcohol'            => ['mc_nutrients_liquids_measures',                [['dietary_alcohol'            , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryBiotin'             => ['mc_nutrients_vitamins_measures',               [['dietary_biotin'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryCaffeine'           => ['mc_nutrients_subsets_measures',                [['dietary_caffeine'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryCalcium '           => ['mc_nutrients_minerals_measures',               [['dietary_calcium'            , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryCarbohydrates'      => ['mc_nutrients_macro_measures',                  [['dietary_carbohydrates'      , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryChloride'           => ['mc_nutrients_minerals_measures',               [['dietary_chloride'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryCholesterol'        => ['mc_nutrients_subsets_measures',                [['dietary_cholesterol'        , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryChromium'           => ['mc_nutrients_minerals_measures',               [['dietary_chromium'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryCopper'             => ['mc_nutrients_minerals_measures',               [['dietary_copper'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryEnergyConsumed'     => ['mc_nutrients_macro_measures',                  [['dietary_energy_consumed'    , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryFatMonounsaturated' => ['mc_nutrients_subsets_measures',                [['dietary_fat_monounsaturated', 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryFatPolyunsaturated' => ['mc_nutrients_subsets_measures',                [['dietary_fat_polyunsaturated', 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryFatSaturated'       => ['mc_nutrients_subsets_measures',                [['dietary_fat_saturated'      , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryFatTotal'           => ['mc_nutrients_macro_measures',                  [['dietary_fat_total'          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryFiber'              => ['mc_nutrients_subsets_measures',                [['dietary_fiber'              , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryFolate'             => ['mc_nutrients_vitamins_measures',               [['dietary_folate'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryIodine'             => ['mc_nutrients_minerals_measures',               [['dietary_iodine'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryIron'               => ['mc_nutrients_minerals_measures',               [['dietary_iron'               , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryMagnesium'          => ['mc_nutrients_minerals_measures',               [['dietary_magnesium'          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryManganese'          => ['mc_nutrients_minerals_measures',               [['dietary_manganese'          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryMolybdenum'         => ['mc_nutrients_minerals_measures',               [['dietary_molybdenum'         , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryNiacin'             => ['mc_nutrients_minerals_measures',               [['dietary_niacin'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryPantothenicAcid'    => ['mc_nutrients_vitamins_measures',               [['dietary_pantothenic_acid'   , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryPhosphorus'         => ['mc_nutrients_minerals_measures',               [['dietary_phosphorus'         , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryPotassium'          => ['mc_nutrients_minerals_measures',               [['dietary_potassium'          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryProtein'            => ['mc_nutrients_macro_measures',                  [['dietary_protein'            , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryRiboflavin'         => ['mc_nutrients_vitamins_measures',               [['dietary_riboflavin'         , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietarySelenium'           => ['mc_nutrients_minerals_measures',               [['dietary_selenium'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietarySodium'             => ['mc_nutrients_minerals_measures',               [['dietary_sodium'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryThiamin'            => ['mc_nutrients_vitamins_measures',               [['dietary_thiamin'            , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietarySugar'              => ['mc_nutrients_subsets_measures',                [['dietary_sugar'              , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminA'           => ['mc_nutrients_vitamins_measures',               [['dietary_vitamina'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminB12'         => ['mc_nutrients_vitamins_measures',               [['dietary_vitaminb12'         , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminB6'          => ['mc_nutrients_vitamins_measures',               [['dietary_vitaminb6'          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminC'           => ['mc_nutrients_vitamins_measures',               [['dietary_vitaminc'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminD'           => ['mc_nutrients_vitamins_measures',               [['dietary_vitamind'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminE'           => ['mc_nutrients_vitamins_measures',               [['dietary_vitamine'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryVitaminK'           => ['mc_nutrients_vitamins_measures',               [['dietary_vitamink'           , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryWater'              => ['mc_nutrients_liquids_measures',                [['dietary_water'              , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDietaryZinc'               => ['mc_nutrients_minerals_measures',               [['dietary_zinc'               , 'unnamed_val']]],
    'HKQuantityTypeIdentifierDistanceWalkingRunning'    => ['mc_activity_measures',                         [['activity_type', 'activity' ], ['activity_duration', 'duration_ts'], ['activity_value', 'renamedjson']]],
    'HKQuantityTypeIdentifierElectrodermalActivity'     => ['mc_misc_measures',                             [['electrodermal_activity'      , 'unnamed_val']]],
    'HKQuantityTypeIdentifierFlightsClimbed'            => ['mc_activity_measures',                         [['activity_type', 'activity' ], ['activity_duration', 'zero'], ['activity_value', 'mappedjson']]],
    'HKQuantityTypeIdentifierForcedExpiratoryVolume1'   => ['mc_lung_measures',                             [['forced_expiratory_volume_one_second' , 'unnamed_val']]],
    'HKQuantityTypeIdentifierForcedVitalCapacity'       => ['mc_lung_measures',                             [['forced_vital_capacity'               , 'unnamed_val']]],
    'HKQuantityTypeIdentifierHeartRate'                 => ['mc_heart_rate_measures',                       [['heart_rate'                          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierHeight'                    => ['mc_body_measures',                             [['body_height'                         , 'unnamed_val']]],
    'HKQuantityTypeIdentifierInhalerUsage'              => ['mc_lung_measures',                             [['inhaler_usage'                       , 'count'      ]]],
    'HKQuantityTypeIdentifierLeanBodyMass'              => ['mc_body_measures',                             [['lean_body_mass'                      , 'unnamed_val']]],
    'HKQuantityTypeIdentifierNikeFuel'                  => ['mc_misc_measures',                             [['nike_fuel'                           , 'count'      ]]],
    'HKQuantityTypeIdentifierNumberOfTimesFallen'       => ['mc_misc_measures',                             [['number_of_times_fallen'              , 'count'      ]]],
    'HKQuantityTypeIdentifierOxygenSaturation'          => ['mc_blood_measures',                            [['blood_oxygen_saturation'             , 'unnamed_val']]],
    'HKQuantityTypeIdentifierPeakExpiratoryFlowRate'    => ['mc_lung_measures',                             [['peak_expiratory_flow'                , 'unnamed_val']]],
    'HKQuantityTypeIdentifierPeripheralPerfusionIndex'  => ['mc_misc_measures',                             [['peripheral_perfusion_index'          , 'unnamed_val']]],
    'HKQuantityTypeIdentifierRespiratoryRate'           => ['mc_lung_measures',                             [['respiratory_rate'                    , 'unnamed_val']]],
    'HKQuantityTypeIdentifierUVExposure'                => ['mc_light_measures',                            [['uv_exposure'                         , 'count'      ]]]
  }

named = {
    'blood_glucose'            => ['mc_blood_measures',          [['blood_glucose'           , 'named_val']]],
    'blood_pressure'           => ['mc_blood_pressure_measures', [['systolic_blood_pressure' , 'named_val'], ['diastolic_blood_pressure' , 'named_val']]],
    'body_fat_percentage'      => ['mc_body_measures',           [['body_fat_percentage'     , 'named_val']]],
    'body_height'              => ['mc_body_measures',           [['body_height'             , 'named_val']]],
    'body_mass_index'          => ['mc_body_measures',           [['body_mass_index'         , 'named_val']]],
    'body_temperature'         => ['mc_body_measures',           [['body_temperature'        , 'named_val']]],
    'body_weight'              => ['mc_body_measures',           [['body_weight'             , 'named_val']]],
    'diastolic_blood_pressure' => ['mc_blood_pressure_measures', [['diastolic_blood_pressure', 'named_val']]],
    'kcal_burned'              => ['mc_energy_measures',         [['active_energy_burned'    , 'named_val']]],
    'heart_rate'               => ['mc_heart_rate_measures',     [['heart_rate'              , 'named_val']]],
    'oxygen_saturation'        => ['mc_blood_measures',          [['blood_oxygen_saturation' , 'named_val']]],
    'respiratory_rate'         => ['mc_lung_measures',           [['respiratory_rate'        , 'named_val']]],
    'sleep_duration'           => ['mc_sleep_measures',          [['sleep_duration'          , 'named_val']]],
    'step_count'               => ['mc_activity_measures',       [['activity_type', 'activity' ], ['activity_duration', 'duration_ts'], ['activity_value' , 'namedjson']]],
    'systolic_blood_pressure'  => ['mc_blood_pressure_measures', [['systolic_blood_pressure' , 'named_val']]]
  }

categories = {
    'HKCategoryTypeIdentifierAppleStandHour' => ['mc_misc_measures', [['apple_stand_hour', ['duration_ts', 'Standing']]]],
  }

activities = {
    'HKWorkoutActivityTypePreparationAndRecovery'       => ['mc_meal_measures'     , [['meal_duration', 'duration'], ['food_type', 'jsonmetadata' ]]],
    'HKWorkoutActivityTypeAmericanFootball'             => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeArchery'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeAustralianFootball'           => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeBadminton'                    => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeBaseball'                     => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeBasketball'                   => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeBowling'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeBoxing'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeClimbing'                     => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeCricket'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeCrossTraining'                => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeCurling'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeCycling'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeDance'                        => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeDanceInspiredTraining'        => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeElliptical'                   => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeEquestrianSports'             => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeFencing'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeFishing'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeFunctionalStrengthTraining'   => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeGolf'                         => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeGymnastics'                   => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeHandball'                     => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeHiking'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeHockey'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeHunting'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeLacrosse'                     => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeMartialArts'                  => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeMindAndBody'                  => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeMixedMetabolicCardioTraining' => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypePaddleSports'                 => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypePlay'                         => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeRacquetball'                  => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeRowing'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeRugby'                        => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeRunning'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSailing'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSkatingSports'                => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSnowSports'                   => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSoccer'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSoftball'                     => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSquash'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeStairClimbing'                => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSurfingSports'                => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeSwimming'                     => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeTableTennis'                  => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeTennis'                       => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeTrackAndField'                => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeTraditionalStrengthTraining'  => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeVolleyball'                   => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeWalking'                      => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeWaterFitness'                 => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeWaterPolo'                    => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeWaterSports'                  => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeWrestling'                    => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeYoga'                         => ['mc_activity_measures' , activity_fields],
    'HKWorkoutActivityTypeOther'                        => ['mc_activity_measures' , activity_fields],
}

dbattrs = ( unnamed.map    { |n, vals| vals[1][0][0] } +
            named.map      { |n, vals| vals[1][0][0] } +
            categories.map { |n, vals| vals[1][0][0] } +
            activities.map { |n, vals| vals[1][0][0] }
          ).uniq.flatten()

dbprojections = ( unnamed.map    { |n, vals| vals[1].map {|k,v| k }.join('_') } +
                  named.map      { |n, vals| vals[1].map {|k,v| k }.join('_') } +
                  categories.map { |n, vals| vals[1].map {|k,v| k }.join('_') } +
                  activities.map { |n, vals| vals[1].map {|k,v| k }.join('_') }
                ).uniq.flatten()

$attrkeys = Hash[dbprojections.each_with_index.map {|k,v| [k,v] }]

projections_by_rel = Hash[( unnamed.map    { |_, vals| vals } +
                            named.map      { |_, vals| vals } +
                            categories.map { |_, vals| vals } +
                            activities.map { |_, vals| vals }
                          ).uniq.group_by {|rel,_| rel }.map {|rel,v| [rel, v.map {|field| field[1] }.uniq ]} ]

def extractor(k,v)
  $extractors[k][0].is_a?(String) ? $extractors[k][0] : $extractors[k][0].call(v)
end

def extractor_type(k)
  $extractors[k][1]
end

def category_extractor(k,v,cv)
  $category_extractors[k][0].is_a?(String) ? $category_extractors[k][0] : $category_extractors[k][0].call(v, cv)
end

def category_extractor_type(k)
  $category_extractors[k][1]
end

def timestamp_attr(as_end)
  "case when body#>'{body, effective_time_frame}' ? 'time_interval' then #{$extractors[as_end ? "end_time" : "start_time"][0]}" +
  " when body#>'{body, effective_time_frame}' ? 'date_time' then #{$extractors["date_time"][0]}" +
  " else null end"
end

def measure_case_array(as_literal, id, tbl, fields)
  indent = (' ' * 24)
  projection_id   = fields.map {|k,_| k }.join('_')
  pfx_fields      = [ "to_json('#{tbl}'::text)", "to_json(#{$attrkeys[projection_id]})"]
  when_clause     = as_literal ? "when '\"#{id}\"'" : "when body->'body' ? '#{id}'"
  non_null_fields = pfx_fields + fields.map { |_,xtor| extractor(xtor, id) } + ["body#>'{body, metadata}'"]
  then_fields     = non_null_fields + ['null'] * ($max_fields - non_null_fields.length)
  then_clause     = "then ARRAY[#{then_fields.join(', ')}]::jsonb[]"
  [when_clause, then_clause].map { |l| indent + l }.join("\n")
end

def category_case_array(as_literal, id, tbl, fields)
  indent = (' ' * 24)
  projection_id   = fields.map {|k,_,_| k }.join('_')
  pfx_fields      = [ "to_json('#{tbl}'::text)", "to_json(#{$attrkeys[projection_id]})"]
  when_clause     = as_literal ? "when '\"#{id}\"'" : "when body->'body' ? '#{id}'"
  non_null_fields = pfx_fields + fields.map { |_,(xtor,catval)| category_extractor(xtor, id, catval) } + ["body#>'{body, metadata}'"]
  then_fields     = non_null_fields + ['null'] * ($max_fields - non_null_fields.length)
  then_clause     = "then ARRAY[#{then_fields.join(', ')}]::jsonb[]"
  [when_clause, then_clause].map { |l| indent + l }.join("\n")
end

def upsert_query(tbl, projections)
  indexed_prjs = projections.map {|p|
    projection_name = p.map {|k,_| k }.join('_')
    projection_id = $attrkeys[projection_name]
    p.each_with_index.map {|(attr, xtor), i|
      if xtor.is_a?(String)
        [i, [projection_id, attr, extractor_type(xtor)]]
      else
        [i, [projection_id, attr, category_extractor_type(xtor[0])]]
      end
    }
  }

  # Collect projection id, attr, type triples for each slot in the measures array.
  fields_by_idx = {}
  indexed_prjs.each { |ip|
    ip.each {|i,g|
      fields_by_idx[i] = (fields_by_idx.has_key?(i) ? fields_by_idx[i] : []) + [g]
    }
  }

  fields_by_idx.keys.each { |k|
    fields_by_idx[k] = fields_by_idx[k].uniq
  }

  # Create case statements for json object keys, per slot.
  labels_by_idx = (fields_by_idx.to_a.sort { |x,y| x[0] <=> y[0] }.map { |i, prjs|
      whens = prjs.map {|idx,a,_| "when #{idx} then '#{a}'" }.join(' ')
      "(case (measure_array->>1)::int #{whens} else 'skip' end)"
    }) + ["'metadata'"]

  labels_by_idx += ['\'skip\''] * ($max_fields - (2 + labels_by_idx.length))
  labels_by_idx_str = labels_by_idx.join(', ')

  clean_query = <<SUBQUERY
        , #{tbl}_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', #{labels_by_idx_str}]::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = '#{tbl}'
        )
SUBQUERY

  field_types_by_name = Hash[fields_by_idx.map {|i,prjs| prjs.map {|p| [p[1], p[2]]} }.flatten(1)
                                          .group_by {|attr,_| attr }.map {|a,g| [a, g.map {|v| v[1]}.uniq]}]

  if field_types_by_name.all? {|_,v| v.length <= 1 }
    upsert_meta  = field_types_by_name.map { |attr,tys| "#{attr} = excluded.#{attr}" }.join(', ')
    insert_attrs     = field_types_by_name.to_a.sort {|x,y| x[0] <=> y[0]}.map {|attr,_| attr}
    insert_attrs_str = insert_attrs.join(', ')
    insert_meta      = insert_attrs.map { |a|
                          ty = field_types_by_name[a][0]
                          cast_op = ty.start_with?("text::") ? ty.sub(/text\:\:/, '') : ty
                          "(case when vals->>'#{a}' = 'null' then null else (vals->>'#{a}')::#{cast_op} end)"
                        }.join(', ')

    upsert_query = <<SUBQ
        , #{tbl}_load as (
          insert into #{tbl} as original(sid, udbid, ts, uuid, #{insert_attrs_str}, metadata)
          select M.msid, M.uid, M.sts, M.uuid, #{insert_meta}, (vals->>'metadata')::jsonb
          from  #{tbl}_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                #{upsert_meta}
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
SUBQ
    clean_query + upsert_query

  else
    puts "Inconsistent extractor types for #{tbl}:"
    field_types_by_name.each { |k,v| puts "#{k} => #{v}" }
    nil
  end
end


unnamed_cases  = unnamed.map    { |n, vals| measure_case_array(true, n, vals[0], vals[1]) }.join("\n")
named_cases    = named.map      { |n, vals| measure_case_array(false, n, vals[0], vals[1]) }.join("\n")
category_cases = categories.map { |n, vals| category_case_array(true, n, vals[0], vals[1]) }.join("\n")
activity_cases = activities.map { |n, vals| measure_case_array(true, n, vals[0], vals[1]) }.uniq.join("\n")

# upserts = (unnamed.values + named.values + activities.values).uniq.map { |vals| upsert_query(vals[0], vals[1]) }.uniq.join

upserts = projections_by_rel.map {|rel,prjs| upsert_query(rel, prjs) }.join


stats_indent = ' ' * 12
statattrs = projections_by_rel.map { |rel,_| "#{rel}_load" }
stats     = statattrs.map {|cte| "            select '#{cte}' as load_id, count(*) as value from #{cte}" }.join("\n            union\n")

mismatch_fields = (["to_json('unknown'::text)", "to_json(-1)"] + ['null'] * ($max_fields - 2)).join(', ')

$granola_etl_udf = <<FUNCTION
create or replace function measures_mc_granola_etl(etl_job_id bigint, max_rows_to_load bigint) returns void as $$
declare
measure_threshold bigint;
begin
    select last_measure_id into measure_threshold
    from   measures_etl_progress
    where  last_measure_id >= 0 and dataset_type = #{$dataset_types["granola"]}
    order by job_id desc limit 1;

    if not found then
        measure_threshold := 0;
    end if;

    raise notice 'Measures (Granola) ETL processing from measure threshold: %', measure_threshold;

    with measure_arrays as (
          select mc_granola_measures.id as msid,
                udbid as uid,
                #{timestamp_attr(false)}::text::timestamptz as sts,
                #{timestamp_attr(true)}::text::timestamptz as ets,
                (body#>>'{header, id}')::uuid as uuid,
                (case
                  when body->'body' ? 'quantity_type'
                    then
                      (case (body#>'{body, quantity_type}')::text
#{unnamed_cases}
                        else ARRAY[#{mismatch_fields}]::jsonb[]
                        end)
                  when body->'body' ? 'activity_name'
                    then
                      (case (body#>'{body, activity_name}')::text
#{activity_cases}
                        else ARRAY[#{mismatch_fields}]::jsonb[]
                        end)
                  when body->'body' ? 'category_type'
                    then
                      (case (body#>'{body, category_type}')::text
#{category_cases}
                        else ARRAY[#{mismatch_fields}]::jsonb[]
                       end)
                  else
                      (case
#{named_cases}
                        else ARRAY[#{mismatch_fields}]::jsonb[]
                        end)
                  end) as measure_array
          from mc_granola_measures, users
          where users.id = decode(trim(both '\"' from (body->'userid')::text), 'base64')
          and   mc_granola_measures.id > measure_threshold
          order by mc_granola_measures.id
          limit max_rows_to_load
        ),
        cleaned_measure_arrays as (
          select measure_array[2]::text::bigint as prjid, msid, uid, sts, ets, uuid, array_to_json(measure_array) as measure_array
          from   measure_arrays
          where (measure_array[1])::text != '"unknown"'
        )
#{upserts.chomp}
        , etl_stats as (
          insert into measures_etl_jobs
          select etl_job_id, R.load_id, R.value from
          (
            select 'total' as load_id, count(*) as value from measure_arrays
            union
#{stats}
          ) as R
          returning job_id, load_id, value
        )
    insert into measures_etl_progress
    select etl_job_id, coalesce(R.last_measure_id, -1) as last_measure_id, (#{$dataset_types["granola"]}::smallint) as dataset_type
    from (select max(msid) as last_measure_id from measure_arrays) as R;
end;
$$ language plpgsql;
FUNCTION

######################################3
#
# MC-JSON ETL UDF generation.
#
# TODO: distinguish between Granola and MC-JSON ETL jobs in measures_etl_progress table.

mc_json_etl_stmts = []
mc_etl_stats_stmts = []

$dbschema_by_rel.each { |rel, attrs_types|
  attrs = attrs_types.map { |a,_| "JSREC.#{a}" }.join(", ")
  rel_schema = attrs_types.map { |a,t| "#{a} #{t}" }.join(", ")

  update_assignments = attrs_types.map { |a,_| "#{a} = coalesce(excluded.#{a}, original.#{a})"}

  load_query = <<QUERY
  #{rel}_load as (
    insert into #{rel} as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, #{attrs}, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'#{rel}') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, #{rel_schema}),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set #{update_assignments.join(", ")}
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  )
QUERY

  stats_query = <<QUERY
          select E'#{rel}' as load_id, count(*) as value from #{rel}_load
QUERY

  mc_json_etl_stmts.push(load_query)
  mc_etl_stats_stmts.push(stats_query)
}

$mc_json_etl_udf = <<FUNCTION
create or replace function measures_mc_json_etl(etl_job_id bigint, max_rows_to_load bigint) returns void as $$
declare
measure_threshold bigint;
begin
  select last_measure_id into measure_threshold
  from   measures_etl_progress
  where  last_measure_id >= 0 and dataset_type = #{$dataset_types["mc_json"]}
  order by job_id desc limit 1;

  if not found then
      measure_threshold := 0;
  end if;

  raise notice 'Measures (MC-JSON) ETL processing from measure threshold: %', measure_threshold;

  with etl_candidates as (
    select id as sid, rel, userid, body
    from mc_json_measures
    where mc_json_measures.id > measure_threshold
    order by mc_json_measures.id
    limit max_rows_to_load
  ),
#{mc_json_etl_stmts.map(&:chomp).join(",\n")},
  etl_stats as (
    insert into measures_etl_jobs
    select etl_job_id, R.load_id, R.value
    from (
#{mc_etl_stats_stmts.map(&:chomp).join("\n          union\n")}
    ) R
    returning job_id, load_id, value
  )
  insert into measures_etl_progress
  select etl_job_id, coalesce(R.last_measure_id, -1) as last_measure_id, (#{$dataset_types["mc_json"]}::smallint) as dataset_type
  from (select max(sid) as last_measure_id from etl_candidates) as R;
end;
$$ language plpgsql;
FUNCTION

$mc_etl_launch = <<FUNCTION
create or replace function mc_etl_launch_fn() returns trigger as $$
declare
  etl_batch_size integer;
begin
  select param_value into etl_batch_size from mc_parameters where param_key = E'etl_batch_size';
  if NEW.dataset_type = #{$dataset_types["granola"]} then
    perform measures_mc_granola_etl(NEW.job_id, etl_batch_size);
  elsif NEW.dataset_type = #{$dataset_types["mc_json"]} then
    perform measures_mc_json_etl(NEW.job_id, etl_batch_size);
  end if;

  -- Return values of after trigger are ignored.
  return null;
end;
$$
language plpgsql;

drop trigger if exists on_insert_measures_etl_launch on measures_etl_launch;

create trigger on_insert_measures_etl_launch
after insert on measures_etl_launch
for each row execute procedure mc_etl_launch_fn();
FUNCTION

def main()
  usage = "Usage: #{$PROGRAM_NAME} <output_file>"

  # check that we have a source
  unless ARGV.size == 1
    puts usage
    exit(1)
  end

  output_path = ARGV[0]
  outfile = File.new(output_path, "w+")
  outfile.write($granola_etl_udf)
  outfile.write("\n\n")
  outfile.write($mc_json_etl_udf)
  outfile.write("\n\n")
  outfile.write($mc_etl_launch)
  outfile.close()
end

main
