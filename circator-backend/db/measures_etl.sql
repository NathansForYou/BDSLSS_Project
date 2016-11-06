create or replace function measures_mc_granola_etl(etl_job_id bigint, max_rows_to_load bigint) returns void as $$
declare
measure_threshold bigint;
begin
    select last_measure_id into measure_threshold
    from   measures_etl_progress
    where  last_measure_id >= 0 and dataset_type = 0
    order by job_id desc limit 1;

    if not found then
        measure_threshold := 0;
    end if;

    raise notice 'Measures (Granola) ETL processing from measure threshold: %', measure_threshold;

    with measure_arrays as (
          select mc_granola_measures.id as msid,
                udbid as uid,
                case when body#>'{body, effective_time_frame}' ? 'time_interval' then body#>'{body, effective_time_frame, time_interval, start_date_time}' when body#>'{body, effective_time_frame}' ? 'date_time' then body#>'{body, effective_time_frame, date_time}' else null end::text::timestamptz as sts,
                case when body#>'{body, effective_time_frame}' ? 'time_interval' then body#>'{body, effective_time_frame, time_interval, end_date_time}' when body#>'{body, effective_time_frame}' ? 'date_time' then body#>'{body, effective_time_frame, date_time}' else null end::text::timestamptz as ets,
                (body#>>'{header, id}')::uuid as uuid,
                (case
                  when body->'body' ? 'quantity_type'
                    then
                      (case (body#>'{body, quantity_type}')::text
                        when '"HKQuantityTypeIdentifierActiveEnergyBurned"'
                        then ARRAY[to_json('mc_energy_measures'::text), to_json(0), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierBasalEnergyBurned"'
                        then ARRAY[to_json('mc_energy_measures'::text), to_json(1), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierBloodPressureDiastolic"'
                        then ARRAY[to_json('mc_blood_pressure_measures'::text), to_json(2), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierBloodPressureSystolic"'
                        then ARRAY[to_json('mc_blood_pressure_measures'::text), to_json(3), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryAlcohol"'
                        then ARRAY[to_json('mc_nutrients_liquids_measures'::text), to_json(4), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryBiotin"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(5), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryCaffeine"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(6), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryCalcium "'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(7), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryCarbohydrates"'
                        then ARRAY[to_json('mc_nutrients_macro_measures'::text), to_json(8), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryChloride"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(9), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryCholesterol"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(10), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryChromium"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(11), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryCopper"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(12), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryEnergyConsumed"'
                        then ARRAY[to_json('mc_nutrients_macro_measures'::text), to_json(13), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryFatMonounsaturated"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(14), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryFatPolyunsaturated"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(15), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryFatSaturated"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(16), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryFatTotal"'
                        then ARRAY[to_json('mc_nutrients_macro_measures'::text), to_json(17), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryFiber"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(18), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryFolate"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(19), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryIodine"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(20), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryIron"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(21), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryMagnesium"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(22), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryManganese"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(23), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryMolybdenum"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(24), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryNiacin"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(25), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryPantothenicAcid"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(26), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryPhosphorus"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(27), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryPotassium"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(28), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryProtein"'
                        then ARRAY[to_json('mc_nutrients_macro_measures'::text), to_json(29), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryRiboflavin"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(30), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietarySelenium"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(31), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietarySodium"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(32), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryThiamin"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(33), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietarySugar"'
                        then ARRAY[to_json('mc_nutrients_subsets_measures'::text), to_json(34), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminA"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(35), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminB12"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(36), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminB6"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(37), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminC"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(38), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminD"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(39), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminE"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(40), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryVitaminK"'
                        then ARRAY[to_json('mc_nutrients_vitamins_measures'::text), to_json(41), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryWater"'
                        then ARRAY[to_json('mc_nutrients_liquids_measures'::text), to_json(42), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDietaryZinc"'
                        then ARRAY[to_json('mc_nutrients_minerals_measures'::text), to_json(43), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierDistanceWalkingRunning"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(59), to_json(case when body#>'{body, effective_time_frame}' ? 'date_time' then 0.0 else (extract(epoch from ((body#>'{body, effective_time_frame, time_interval, end_date_time}')::text::timestamptz - (body#>'{body, effective_time_frame, time_interval, start_date_time}')::text::timestamptz)))::real end)::jsonb, json_build_object('distance', (body#>'{body, unit_value, value}')::text::real), body#>'{body, metadata}']::jsonb[]
                        when '"HKQuantityTypeIdentifierElectrodermalActivity"'
                        then ARRAY[to_json('mc_misc_measures'::text), to_json(45), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierFlightsClimbed"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(60), to_json(0), json_build_object('flights', (body#>'{body, count}')::text::real), body#>'{body, metadata}']::jsonb[]
                        when '"HKQuantityTypeIdentifierForcedExpiratoryVolume1"'
                        then ARRAY[to_json('mc_lung_measures'::text), to_json(46), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierForcedVitalCapacity"'
                        then ARRAY[to_json('mc_lung_measures'::text), to_json(47), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierHeartRate"'
                        then ARRAY[to_json('mc_heart_rate_measures'::text), to_json(48), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierHeight"'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(49), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierInhalerUsage"'
                        then ARRAY[to_json('mc_lung_measures'::text), to_json(50), body#>'{body, count}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierLeanBodyMass"'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(51), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierNikeFuel"'
                        then ARRAY[to_json('mc_misc_measures'::text), to_json(52), body#>'{body, count}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierNumberOfTimesFallen"'
                        then ARRAY[to_json('mc_misc_measures'::text), to_json(53), body#>'{body, count}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierOxygenSaturation"'
                        then ARRAY[to_json('mc_blood_measures'::text), to_json(54), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierPeakExpiratoryFlowRate"'
                        then ARRAY[to_json('mc_lung_measures'::text), to_json(55), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierPeripheralPerfusionIndex"'
                        then ARRAY[to_json('mc_misc_measures'::text), to_json(56), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierRespiratoryRate"'
                        then ARRAY[to_json('mc_lung_measures'::text), to_json(57), body#>'{body, unit_value, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when '"HKQuantityTypeIdentifierUVExposure"'
                        then ARRAY[to_json('mc_light_measures'::text), to_json(58), body#>'{body, count}', body#>'{body, metadata}', null, null]::jsonb[]
                        else ARRAY[to_json('unknown'::text), to_json(-1), null, null, null, null]::jsonb[]
                        end)
                  when body->'body' ? 'activity_name'
                    then
                      (case (body#>'{body, activity_name}')::text
                        when '"HKWorkoutActivityTypePreparationAndRecovery"'
                        then ARRAY[to_json('mc_meal_measures'::text), to_json(67), body#>'{body, duration, value}', body#>'{body, metadata}', body#>'{body, metadata}', null]::jsonb[]
                        when '"HKWorkoutActivityTypeAmericanFootball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(1), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeArchery"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(2), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeAustralianFootball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(3), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeBadminton"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(4), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeBaseball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(5), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeBasketball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(6), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeBowling"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(7), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeBoxing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(8), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeClimbing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(9), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeCricket"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(10), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeCrossTraining"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(11), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeCurling"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(12), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeCycling"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(13), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeDance"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(14), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeDanceInspiredTraining"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(15), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeElliptical"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(16), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeEquestrianSports"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(17), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeFencing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(18), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeFishing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(19), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeFunctionalStrengthTraining"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(20), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeGolf"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(21), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeGymnastics"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(22), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeHandball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(23), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeHiking"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(24), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeHockey"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(25), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeHunting"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(26), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeLacrosse"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(27), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeMartialArts"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(28), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeMindAndBody"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(29), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeMixedMetabolicCardioTraining"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(30), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypePaddleSports"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(31), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypePlay"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(32), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeRacquetball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(34), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeRowing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(35), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeRugby"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(36), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeRunning"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(37), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSailing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(38), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSkatingSports"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(39), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSnowSports"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(40), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSoccer"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(41), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSoftball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(42), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSquash"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(43), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeStairClimbing"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(44), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSurfingSports"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(45), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeSwimming"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(46), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeTableTennis"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(47), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeTennis"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(48), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeTrackAndField"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(49), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeTraditionalStrengthTraining"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(50), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeVolleyball"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(51), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeWalking"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(52), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeWaterFitness"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(53), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeWaterPolo"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(54), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeWaterSports"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(55), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeWrestling"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(56), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeYoga"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(57), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        when '"HKWorkoutActivityTypeOther"'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(3000), body#>'{body, duration, value}', json_object(ARRAY['distance', (body#>'{body, distance}')::text, 'kcal_burned', (body#>'{body, kcal_burned}')::text]), body#>'{body, metadata}']::jsonb[]
                        else ARRAY[to_json('unknown'::text), to_json(-1), null, null, null, null]::jsonb[]
                        end)
                  when body->'body' ? 'category_type'
                    then
                      (case (body#>'{body, category_type}')::text
                        when '"HKCategoryTypeIdentifierAppleStandHour"'
                        then ARRAY[to_json('mc_misc_measures'::text), to_json(66), case when body#>>'{category_value}' = E'Standing' then to_json(case when body#>'{body, effective_time_frame}' ? 'date_time' then 0.0 else (extract(epoch from ((body#>'{body, effective_time_frame, time_interval, end_date_time}')::text::timestamptz - (body#>'{body, effective_time_frame, time_interval, start_date_time}')::text::timestamptz)))::real end)::jsonb else to_jsonb(0.0) end, body#>'{body, metadata}', null, null]::jsonb[]
                        else ARRAY[to_json('unknown'::text), to_json(-1), null, null, null, null]::jsonb[]
                       end)
                  else
                      (case
                        when body->'body' ? 'blood_glucose'
                        then ARRAY[to_json('mc_blood_measures'::text), to_json(59), body#>'{body, blood_glucose, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'blood_pressure'
                        then ARRAY[to_json('mc_blood_pressure_measures'::text), to_json(60), body#>'{body, blood_pressure, value}', body#>'{body, blood_pressure, value}', body#>'{body, metadata}', null]::jsonb[]
                        when body->'body' ? 'body_fat_percentage'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(61), body#>'{body, body_fat_percentage, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'body_height'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(49), body#>'{body, body_height, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'body_mass_index'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(62), body#>'{body, body_mass_index, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'body_temperature'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(63), body#>'{body, body_temperature, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'body_weight'
                        then ARRAY[to_json('mc_body_measures'::text), to_json(64), body#>'{body, body_weight, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'diastolic_blood_pressure'
                        then ARRAY[to_json('mc_blood_pressure_measures'::text), to_json(2), body#>'{body, diastolic_blood_pressure, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'kcal_burned'
                        then ARRAY[to_json('mc_energy_measures'::text), to_json(0), body#>'{body, kcal_burned, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'heart_rate'
                        then ARRAY[to_json('mc_heart_rate_measures'::text), to_json(48), body#>'{body, heart_rate, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'oxygen_saturation'
                        then ARRAY[to_json('mc_blood_measures'::text), to_json(54), body#>'{body, oxygen_saturation, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'respiratory_rate'
                        then ARRAY[to_json('mc_lung_measures'::text), to_json(57), body#>'{body, respiratory_rate, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'sleep_duration'
                        then ARRAY[to_json('mc_sleep_measures'::text), to_json(65), body#>'{body, sleep_duration, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        when body->'body' ? 'step_count'
                        then ARRAY[to_json('mc_activity_measures'::text), to_json(44), to_json(58), to_json(case when body#>'{body, effective_time_frame}' ? 'date_time' then 0.0 else (extract(epoch from ((body#>'{body, effective_time_frame, time_interval, end_date_time}')::text::timestamptz - (body#>'{body, effective_time_frame, time_interval, start_date_time}')::text::timestamptz)))::real end)::jsonb, json_build_object('step_count', (body#>'{body, step_count}')::text::real), body#>'{body, metadata}']::jsonb[]
                        when body->'body' ? 'systolic_blood_pressure'
                        then ARRAY[to_json('mc_blood_pressure_measures'::text), to_json(3), body#>'{body, systolic_blood_pressure, value}', body#>'{body, metadata}', null, null]::jsonb[]
                        else ARRAY[to_json('unknown'::text), to_json(-1), null, null, null, null]::jsonb[]
                        end)
                  end) as measure_array
          from mc_granola_measures, users
          where users.id = decode(trim(both '"' from (body->'userid')::text), 'base64')
          and   mc_granola_measures.id > measure_threshold
          order by mc_granola_measures.id
          limit max_rows_to_load
        ),
        cleaned_measure_arrays as (
          select measure_array[2]::text::bigint as prjid, msid, uid, sts, ets, uuid, array_to_json(measure_array) as measure_array
          from   measure_arrays
          where (measure_array[1])::text != '"unknown"'
        )
        , mc_energy_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 0 then 'active_energy_burned' when 1 then 'basal_energy_burned' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_energy_measures'
        )
        , mc_energy_measures_load as (
          insert into mc_energy_measures as original(sid, udbid, ts, uuid, active_energy_burned, basal_energy_burned, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'active_energy_burned' = 'null' then null else (vals->>'active_energy_burned')::real end), (case when vals->>'basal_energy_burned' = 'null' then null else (vals->>'basal_energy_burned')::real end), (vals->>'metadata')::jsonb
          from  mc_energy_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                active_energy_burned = excluded.active_energy_burned, basal_energy_burned = excluded.basal_energy_burned
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_blood_pressure_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 2 then 'diastolic_blood_pressure' when 3 then 'systolic_blood_pressure' when 60 then 'systolic_blood_pressure' else 'skip' end), (case (measure_array->>1)::int when 60 then 'diastolic_blood_pressure' else 'skip' end), 'metadata', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_blood_pressure_measures'
        )
        , mc_blood_pressure_measures_load as (
          insert into mc_blood_pressure_measures as original(sid, udbid, ts, uuid, diastolic_blood_pressure, systolic_blood_pressure, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'diastolic_blood_pressure' = 'null' then null else (vals->>'diastolic_blood_pressure')::real end), (case when vals->>'systolic_blood_pressure' = 'null' then null else (vals->>'systolic_blood_pressure')::real end), (vals->>'metadata')::jsonb
          from  mc_blood_pressure_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                diastolic_blood_pressure = excluded.diastolic_blood_pressure, systolic_blood_pressure = excluded.systolic_blood_pressure
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_nutrients_liquids_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 4 then 'dietary_alcohol' when 42 then 'dietary_water' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_nutrients_liquids_measures'
        )
        , mc_nutrients_liquids_measures_load as (
          insert into mc_nutrients_liquids_measures as original(sid, udbid, ts, uuid, dietary_alcohol, dietary_water, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'dietary_alcohol' = 'null' then null else (vals->>'dietary_alcohol')::real end), (case when vals->>'dietary_water' = 'null' then null else (vals->>'dietary_water')::real end), (vals->>'metadata')::jsonb
          from  mc_nutrients_liquids_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                dietary_alcohol = excluded.dietary_alcohol, dietary_water = excluded.dietary_water
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_nutrients_vitamins_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 5 then 'dietary_biotin' when 19 then 'dietary_folate' when 26 then 'dietary_pantothenic_acid' when 30 then 'dietary_riboflavin' when 33 then 'dietary_thiamin' when 35 then 'dietary_vitamina' when 36 then 'dietary_vitaminb12' when 37 then 'dietary_vitaminb6' when 38 then 'dietary_vitaminc' when 39 then 'dietary_vitamind' when 40 then 'dietary_vitamine' when 41 then 'dietary_vitamink' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_nutrients_vitamins_measures'
        )
        , mc_nutrients_vitamins_measures_load as (
          insert into mc_nutrients_vitamins_measures as original(sid, udbid, ts, uuid, dietary_biotin, dietary_folate, dietary_pantothenic_acid, dietary_riboflavin, dietary_thiamin, dietary_vitamina, dietary_vitaminb12, dietary_vitaminb6, dietary_vitaminc, dietary_vitamind, dietary_vitamine, dietary_vitamink, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'dietary_biotin' = 'null' then null else (vals->>'dietary_biotin')::real end), (case when vals->>'dietary_folate' = 'null' then null else (vals->>'dietary_folate')::real end), (case when vals->>'dietary_pantothenic_acid' = 'null' then null else (vals->>'dietary_pantothenic_acid')::real end), (case when vals->>'dietary_riboflavin' = 'null' then null else (vals->>'dietary_riboflavin')::real end), (case when vals->>'dietary_thiamin' = 'null' then null else (vals->>'dietary_thiamin')::real end), (case when vals->>'dietary_vitamina' = 'null' then null else (vals->>'dietary_vitamina')::real end), (case when vals->>'dietary_vitaminb12' = 'null' then null else (vals->>'dietary_vitaminb12')::real end), (case when vals->>'dietary_vitaminb6' = 'null' then null else (vals->>'dietary_vitaminb6')::real end), (case when vals->>'dietary_vitaminc' = 'null' then null else (vals->>'dietary_vitaminc')::real end), (case when vals->>'dietary_vitamind' = 'null' then null else (vals->>'dietary_vitamind')::real end), (case when vals->>'dietary_vitamine' = 'null' then null else (vals->>'dietary_vitamine')::real end), (case when vals->>'dietary_vitamink' = 'null' then null else (vals->>'dietary_vitamink')::real end), (vals->>'metadata')::jsonb
          from  mc_nutrients_vitamins_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                dietary_biotin = excluded.dietary_biotin, dietary_folate = excluded.dietary_folate, dietary_pantothenic_acid = excluded.dietary_pantothenic_acid, dietary_riboflavin = excluded.dietary_riboflavin, dietary_thiamin = excluded.dietary_thiamin, dietary_vitamina = excluded.dietary_vitamina, dietary_vitaminb12 = excluded.dietary_vitaminb12, dietary_vitaminb6 = excluded.dietary_vitaminb6, dietary_vitaminc = excluded.dietary_vitaminc, dietary_vitamind = excluded.dietary_vitamind, dietary_vitamine = excluded.dietary_vitamine, dietary_vitamink = excluded.dietary_vitamink
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_nutrients_subsets_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 6 then 'dietary_caffeine' when 10 then 'dietary_cholesterol' when 14 then 'dietary_fat_monounsaturated' when 15 then 'dietary_fat_polyunsaturated' when 16 then 'dietary_fat_saturated' when 18 then 'dietary_fiber' when 34 then 'dietary_sugar' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_nutrients_subsets_measures'
        )
        , mc_nutrients_subsets_measures_load as (
          insert into mc_nutrients_subsets_measures as original(sid, udbid, ts, uuid, dietary_caffeine, dietary_cholesterol, dietary_fat_monounsaturated, dietary_fat_polyunsaturated, dietary_fat_saturated, dietary_fiber, dietary_sugar, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'dietary_caffeine' = 'null' then null else (vals->>'dietary_caffeine')::real end), (case when vals->>'dietary_cholesterol' = 'null' then null else (vals->>'dietary_cholesterol')::real end), (case when vals->>'dietary_fat_monounsaturated' = 'null' then null else (vals->>'dietary_fat_monounsaturated')::real end), (case when vals->>'dietary_fat_polyunsaturated' = 'null' then null else (vals->>'dietary_fat_polyunsaturated')::real end), (case when vals->>'dietary_fat_saturated' = 'null' then null else (vals->>'dietary_fat_saturated')::real end), (case when vals->>'dietary_fiber' = 'null' then null else (vals->>'dietary_fiber')::real end), (case when vals->>'dietary_sugar' = 'null' then null else (vals->>'dietary_sugar')::real end), (vals->>'metadata')::jsonb
          from  mc_nutrients_subsets_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                dietary_caffeine = excluded.dietary_caffeine, dietary_cholesterol = excluded.dietary_cholesterol, dietary_fat_monounsaturated = excluded.dietary_fat_monounsaturated, dietary_fat_polyunsaturated = excluded.dietary_fat_polyunsaturated, dietary_fat_saturated = excluded.dietary_fat_saturated, dietary_fiber = excluded.dietary_fiber, dietary_sugar = excluded.dietary_sugar
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_nutrients_minerals_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 7 then 'dietary_calcium' when 9 then 'dietary_chloride' when 11 then 'dietary_chromium' when 12 then 'dietary_copper' when 20 then 'dietary_iodine' when 21 then 'dietary_iron' when 22 then 'dietary_magnesium' when 23 then 'dietary_manganese' when 24 then 'dietary_molybdenum' when 25 then 'dietary_niacin' when 27 then 'dietary_phosphorus' when 28 then 'dietary_potassium' when 31 then 'dietary_selenium' when 32 then 'dietary_sodium' when 43 then 'dietary_zinc' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_nutrients_minerals_measures'
        )
        , mc_nutrients_minerals_measures_load as (
          insert into mc_nutrients_minerals_measures as original(sid, udbid, ts, uuid, dietary_calcium, dietary_chloride, dietary_chromium, dietary_copper, dietary_iodine, dietary_iron, dietary_magnesium, dietary_manganese, dietary_molybdenum, dietary_niacin, dietary_phosphorus, dietary_potassium, dietary_selenium, dietary_sodium, dietary_zinc, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'dietary_calcium' = 'null' then null else (vals->>'dietary_calcium')::real end), (case when vals->>'dietary_chloride' = 'null' then null else (vals->>'dietary_chloride')::real end), (case when vals->>'dietary_chromium' = 'null' then null else (vals->>'dietary_chromium')::real end), (case when vals->>'dietary_copper' = 'null' then null else (vals->>'dietary_copper')::real end), (case when vals->>'dietary_iodine' = 'null' then null else (vals->>'dietary_iodine')::real end), (case when vals->>'dietary_iron' = 'null' then null else (vals->>'dietary_iron')::real end), (case when vals->>'dietary_magnesium' = 'null' then null else (vals->>'dietary_magnesium')::real end), (case when vals->>'dietary_manganese' = 'null' then null else (vals->>'dietary_manganese')::real end), (case when vals->>'dietary_molybdenum' = 'null' then null else (vals->>'dietary_molybdenum')::real end), (case when vals->>'dietary_niacin' = 'null' then null else (vals->>'dietary_niacin')::real end), (case when vals->>'dietary_phosphorus' = 'null' then null else (vals->>'dietary_phosphorus')::real end), (case when vals->>'dietary_potassium' = 'null' then null else (vals->>'dietary_potassium')::real end), (case when vals->>'dietary_selenium' = 'null' then null else (vals->>'dietary_selenium')::real end), (case when vals->>'dietary_sodium' = 'null' then null else (vals->>'dietary_sodium')::real end), (case when vals->>'dietary_zinc' = 'null' then null else (vals->>'dietary_zinc')::real end), (vals->>'metadata')::jsonb
          from  mc_nutrients_minerals_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                dietary_calcium = excluded.dietary_calcium, dietary_chloride = excluded.dietary_chloride, dietary_chromium = excluded.dietary_chromium, dietary_copper = excluded.dietary_copper, dietary_iodine = excluded.dietary_iodine, dietary_iron = excluded.dietary_iron, dietary_magnesium = excluded.dietary_magnesium, dietary_manganese = excluded.dietary_manganese, dietary_molybdenum = excluded.dietary_molybdenum, dietary_niacin = excluded.dietary_niacin, dietary_phosphorus = excluded.dietary_phosphorus, dietary_potassium = excluded.dietary_potassium, dietary_selenium = excluded.dietary_selenium, dietary_sodium = excluded.dietary_sodium, dietary_zinc = excluded.dietary_zinc
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_nutrients_macro_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 8 then 'dietary_carbohydrates' when 13 then 'dietary_energy_consumed' when 17 then 'dietary_fat_total' when 29 then 'dietary_protein' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_nutrients_macro_measures'
        )
        , mc_nutrients_macro_measures_load as (
          insert into mc_nutrients_macro_measures as original(sid, udbid, ts, uuid, dietary_carbohydrates, dietary_energy_consumed, dietary_fat_total, dietary_protein, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'dietary_carbohydrates' = 'null' then null else (vals->>'dietary_carbohydrates')::real end), (case when vals->>'dietary_energy_consumed' = 'null' then null else (vals->>'dietary_energy_consumed')::real end), (case when vals->>'dietary_fat_total' = 'null' then null else (vals->>'dietary_fat_total')::real end), (case when vals->>'dietary_protein' = 'null' then null else (vals->>'dietary_protein')::real end), (vals->>'metadata')::jsonb
          from  mc_nutrients_macro_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                dietary_carbohydrates = excluded.dietary_carbohydrates, dietary_energy_consumed = excluded.dietary_energy_consumed, dietary_fat_total = excluded.dietary_fat_total, dietary_protein = excluded.dietary_protein
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_activity_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 44 then 'activity_type' else 'skip' end), (case (measure_array->>1)::int when 44 then 'activity_duration' else 'skip' end), (case (measure_array->>1)::int when 44 then 'activity_value' else 'skip' end), 'metadata']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_activity_measures'
        )
        , mc_activity_measures_load as (
          insert into mc_activity_measures as original(sid, udbid, ts, uuid, activity_duration, activity_type, activity_value, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'activity_duration' = 'null' then null else (vals->>'activity_duration')::real end), (case when vals->>'activity_type' = 'null' then null else (vals->>'activity_type')::integer end), (case when vals->>'activity_value' = 'null' then null else (vals->>'activity_value')::jsonb end), (vals->>'metadata')::jsonb
          from  mc_activity_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                activity_type = excluded.activity_type, activity_duration = excluded.activity_duration, activity_value = excluded.activity_value
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_misc_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 45 then 'electrodermal_activity' when 52 then 'nike_fuel' when 53 then 'number_of_times_fallen' when 56 then 'peripheral_perfusion_index' when 66 then 'apple_stand_hour' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_misc_measures'
        )
        , mc_misc_measures_load as (
          insert into mc_misc_measures as original(sid, udbid, ts, uuid, apple_stand_hour, electrodermal_activity, nike_fuel, number_of_times_fallen, peripheral_perfusion_index, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'apple_stand_hour' = 'null' then null else (vals->>'apple_stand_hour')::real end), (case when vals->>'electrodermal_activity' = 'null' then null else (vals->>'electrodermal_activity')::real end), (case when vals->>'nike_fuel' = 'null' then null else (vals->>'nike_fuel')::real end), (case when vals->>'number_of_times_fallen' = 'null' then null else (vals->>'number_of_times_fallen')::real end), (case when vals->>'peripheral_perfusion_index' = 'null' then null else (vals->>'peripheral_perfusion_index')::real end), (vals->>'metadata')::jsonb
          from  mc_misc_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                electrodermal_activity = excluded.electrodermal_activity, nike_fuel = excluded.nike_fuel, number_of_times_fallen = excluded.number_of_times_fallen, peripheral_perfusion_index = excluded.peripheral_perfusion_index, apple_stand_hour = excluded.apple_stand_hour
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_lung_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 46 then 'forced_expiratory_volume_one_second' when 47 then 'forced_vital_capacity' when 50 then 'inhaler_usage' when 55 then 'peak_expiratory_flow' when 57 then 'respiratory_rate' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_lung_measures'
        )
        , mc_lung_measures_load as (
          insert into mc_lung_measures as original(sid, udbid, ts, uuid, forced_expiratory_volume_one_second, forced_vital_capacity, inhaler_usage, peak_expiratory_flow, respiratory_rate, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'forced_expiratory_volume_one_second' = 'null' then null else (vals->>'forced_expiratory_volume_one_second')::real end), (case when vals->>'forced_vital_capacity' = 'null' then null else (vals->>'forced_vital_capacity')::real end), (case when vals->>'inhaler_usage' = 'null' then null else (vals->>'inhaler_usage')::real end), (case when vals->>'peak_expiratory_flow' = 'null' then null else (vals->>'peak_expiratory_flow')::real end), (case when vals->>'respiratory_rate' = 'null' then null else (vals->>'respiratory_rate')::real end), (vals->>'metadata')::jsonb
          from  mc_lung_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                forced_expiratory_volume_one_second = excluded.forced_expiratory_volume_one_second, forced_vital_capacity = excluded.forced_vital_capacity, inhaler_usage = excluded.inhaler_usage, peak_expiratory_flow = excluded.peak_expiratory_flow, respiratory_rate = excluded.respiratory_rate
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_heart_rate_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 48 then 'heart_rate' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_heart_rate_measures'
        )
        , mc_heart_rate_measures_load as (
          insert into mc_heart_rate_measures as original(sid, udbid, ts, uuid, heart_rate, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'heart_rate' = 'null' then null else (vals->>'heart_rate')::real end), (vals->>'metadata')::jsonb
          from  mc_heart_rate_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                heart_rate = excluded.heart_rate
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_body_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 49 then 'body_height' when 51 then 'lean_body_mass' when 61 then 'body_fat_percentage' when 62 then 'body_mass_index' when 63 then 'body_temperature' when 64 then 'body_weight' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_body_measures'
        )
        , mc_body_measures_load as (
          insert into mc_body_measures as original(sid, udbid, ts, uuid, body_fat_percentage, body_height, body_mass_index, body_temperature, body_weight, lean_body_mass, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'body_fat_percentage' = 'null' then null else (vals->>'body_fat_percentage')::real end), (case when vals->>'body_height' = 'null' then null else (vals->>'body_height')::real end), (case when vals->>'body_mass_index' = 'null' then null else (vals->>'body_mass_index')::real end), (case when vals->>'body_temperature' = 'null' then null else (vals->>'body_temperature')::real end), (case when vals->>'body_weight' = 'null' then null else (vals->>'body_weight')::real end), (case when vals->>'lean_body_mass' = 'null' then null else (vals->>'lean_body_mass')::real end), (vals->>'metadata')::jsonb
          from  mc_body_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                body_height = excluded.body_height, lean_body_mass = excluded.lean_body_mass, body_fat_percentage = excluded.body_fat_percentage, body_mass_index = excluded.body_mass_index, body_temperature = excluded.body_temperature, body_weight = excluded.body_weight
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_blood_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 54 then 'blood_oxygen_saturation' when 59 then 'blood_glucose' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_blood_measures'
        )
        , mc_blood_measures_load as (
          insert into mc_blood_measures as original(sid, udbid, ts, uuid, blood_glucose, blood_oxygen_saturation, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'blood_glucose' = 'null' then null else (vals->>'blood_glucose')::real end), (case when vals->>'blood_oxygen_saturation' = 'null' then null else (vals->>'blood_oxygen_saturation')::real end), (vals->>'metadata')::jsonb
          from  mc_blood_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                blood_oxygen_saturation = excluded.blood_oxygen_saturation, blood_glucose = excluded.blood_glucose
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_light_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 58 then 'uv_exposure' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_light_measures'
        )
        , mc_light_measures_load as (
          insert into mc_light_measures as original(sid, udbid, ts, uuid, uv_exposure, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'uv_exposure' = 'null' then null else (vals->>'uv_exposure')::real end), (vals->>'metadata')::jsonb
          from  mc_light_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                uv_exposure = excluded.uv_exposure
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_sleep_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 65 then 'sleep_duration' else 'skip' end), 'metadata', 'skip', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_sleep_measures'
        )
        , mc_sleep_measures_load as (
          insert into mc_sleep_measures as original(sid, udbid, ts, uuid, sleep_duration, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'sleep_duration' = 'null' then null else (vals->>'sleep_duration')::real end), (vals->>'metadata')::jsonb
          from  mc_sleep_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                sleep_duration = excluded.sleep_duration
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , mc_meal_measures_cleaned as (
          select msid, uid, sts, uuid, json_object(
            ARRAY['skip', 'skip', (case (measure_array->>1)::int when 67 then 'meal_duration' else 'skip' end), (case (measure_array->>1)::int when 67 then 'food_type' else 'skip' end), 'metadata', 'skip']::text[],
            ARRAY(select json_array_elements_text(measure_array)))::jsonb as vals
          from cleaned_measure_arrays where measure_array->>0 = 'mc_meal_measures'
        )
        , mc_meal_measures_load as (
          insert into mc_meal_measures as original(sid, udbid, ts, uuid, food_type, meal_duration, metadata)
          select M.msid, M.uid, M.sts, M.uuid, (case when vals->>'food_type' = 'null' then null else (vals->>'food_type')::jsonb end), (case when vals->>'meal_duration' = 'null' then null else (vals->>'meal_duration')::real end), (vals->>'metadata')::jsonb
          from  mc_meal_measures_cleaned as M
          on conflict (sid) do update
            set ts = excluded.ts,
                meal_duration = excluded.meal_duration, food_type = excluded.food_type
            where original.sid = excluded.sid and original.udbid = excluded.udbid
          returning sid
        )
        , etl_stats as (
          insert into measures_etl_jobs
          select etl_job_id, R.load_id, R.value from
          (
            select 'total' as load_id, count(*) as value from measure_arrays
            union
            select 'mc_energy_measures_load' as load_id, count(*) as value from mc_energy_measures_load
            union
            select 'mc_blood_pressure_measures_load' as load_id, count(*) as value from mc_blood_pressure_measures_load
            union
            select 'mc_nutrients_liquids_measures_load' as load_id, count(*) as value from mc_nutrients_liquids_measures_load
            union
            select 'mc_nutrients_vitamins_measures_load' as load_id, count(*) as value from mc_nutrients_vitamins_measures_load
            union
            select 'mc_nutrients_subsets_measures_load' as load_id, count(*) as value from mc_nutrients_subsets_measures_load
            union
            select 'mc_nutrients_minerals_measures_load' as load_id, count(*) as value from mc_nutrients_minerals_measures_load
            union
            select 'mc_nutrients_macro_measures_load' as load_id, count(*) as value from mc_nutrients_macro_measures_load
            union
            select 'mc_activity_measures_load' as load_id, count(*) as value from mc_activity_measures_load
            union
            select 'mc_misc_measures_load' as load_id, count(*) as value from mc_misc_measures_load
            union
            select 'mc_lung_measures_load' as load_id, count(*) as value from mc_lung_measures_load
            union
            select 'mc_heart_rate_measures_load' as load_id, count(*) as value from mc_heart_rate_measures_load
            union
            select 'mc_body_measures_load' as load_id, count(*) as value from mc_body_measures_load
            union
            select 'mc_blood_measures_load' as load_id, count(*) as value from mc_blood_measures_load
            union
            select 'mc_light_measures_load' as load_id, count(*) as value from mc_light_measures_load
            union
            select 'mc_sleep_measures_load' as load_id, count(*) as value from mc_sleep_measures_load
            union
            select 'mc_meal_measures_load' as load_id, count(*) as value from mc_meal_measures_load
          ) as R
          returning job_id, load_id, value
        )
    insert into measures_etl_progress
    select etl_job_id, coalesce(R.last_measure_id, -1) as last_measure_id, (0::smallint) as dataset_type
    from (select max(msid) as last_measure_id from measure_arrays) as R;
end;
$$ language plpgsql;


create or replace function measures_mc_json_etl(etl_job_id bigint, max_rows_to_load bigint) returns void as $$
declare
measure_threshold bigint;
begin
  select last_measure_id into measure_threshold
  from   measures_etl_progress
  where  last_measure_id >= 0 and dataset_type = 1
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
  mc_body_measures_load as (
    insert into mc_body_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.body_weight, JSREC.body_height, JSREC.body_mass_index, JSREC.body_fat_percentage, JSREC.body_temperature, JSREC.basal_body_temperature, JSREC.lean_body_mass, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_body_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, body_weight real, body_height real, body_mass_index real, body_fat_percentage real, body_temperature real, basal_body_temperature real, lean_body_mass real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set body_weight = coalesce(excluded.body_weight, original.body_weight), body_height = coalesce(excluded.body_height, original.body_height), body_mass_index = coalesce(excluded.body_mass_index, original.body_mass_index), body_fat_percentage = coalesce(excluded.body_fat_percentage, original.body_fat_percentage), body_temperature = coalesce(excluded.body_temperature, original.body_temperature), basal_body_temperature = coalesce(excluded.basal_body_temperature, original.basal_body_temperature), lean_body_mass = coalesce(excluded.lean_body_mass, original.lean_body_mass)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_blood_pressure_measures_load as (
    insert into mc_blood_pressure_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.systolic_blood_pressure, JSREC.diastolic_blood_pressure, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_blood_pressure_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, systolic_blood_pressure real, diastolic_blood_pressure real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set systolic_blood_pressure = coalesce(excluded.systolic_blood_pressure, original.systolic_blood_pressure), diastolic_blood_pressure = coalesce(excluded.diastolic_blood_pressure, original.diastolic_blood_pressure)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_sleep_measures_load as (
    insert into mc_sleep_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.sleep_duration, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_sleep_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, sleep_duration real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set sleep_duration = coalesce(excluded.sleep_duration, original.sleep_duration)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_meal_measures_load as (
    insert into mc_meal_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.meal_duration, JSREC.food_type, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_meal_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, meal_duration real, food_type jsonb),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set meal_duration = coalesce(excluded.meal_duration, original.meal_duration), food_type = coalesce(excluded.food_type, original.food_type)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_activity_measures_load as (
    insert into mc_activity_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.activity_duration, JSREC.activity_type, JSREC.activity_value, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_activity_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, activity_duration real, activity_type integer, activity_value jsonb),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set activity_duration = coalesce(excluded.activity_duration, original.activity_duration), activity_type = coalesce(excluded.activity_type, original.activity_type), activity_value = coalesce(excluded.activity_value, original.activity_value)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_light_measures_load as (
    insert into mc_light_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.uv_exposure, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_light_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, uv_exposure real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set uv_exposure = coalesce(excluded.uv_exposure, original.uv_exposure)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_energy_measures_load as (
    insert into mc_energy_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.active_energy_burned, JSREC.basal_energy_burned, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_energy_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, active_energy_burned real, basal_energy_burned real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set active_energy_burned = coalesce(excluded.active_energy_burned, original.active_energy_burned), basal_energy_burned = coalesce(excluded.basal_energy_burned, original.basal_energy_burned)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_blood_measures_load as (
    insert into mc_blood_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.blood_alcohol_content, JSREC.blood_glucose, JSREC.blood_oxygen_saturation, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_blood_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, blood_alcohol_content real, blood_glucose real, blood_oxygen_saturation real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set blood_alcohol_content = coalesce(excluded.blood_alcohol_content, original.blood_alcohol_content), blood_glucose = coalesce(excluded.blood_glucose, original.blood_glucose), blood_oxygen_saturation = coalesce(excluded.blood_oxygen_saturation, original.blood_oxygen_saturation)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_lung_measures_load as (
    insert into mc_lung_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.forced_expiratory_volume_one_second, JSREC.forced_vital_capacity, JSREC.peak_expiratory_flow, JSREC.inhaler_usage, JSREC.respiratory_rate, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_lung_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, forced_expiratory_volume_one_second real, forced_vital_capacity real, peak_expiratory_flow real, inhaler_usage real, respiratory_rate real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set forced_expiratory_volume_one_second = coalesce(excluded.forced_expiratory_volume_one_second, original.forced_expiratory_volume_one_second), forced_vital_capacity = coalesce(excluded.forced_vital_capacity, original.forced_vital_capacity), peak_expiratory_flow = coalesce(excluded.peak_expiratory_flow, original.peak_expiratory_flow), inhaler_usage = coalesce(excluded.inhaler_usage, original.inhaler_usage), respiratory_rate = coalesce(excluded.respiratory_rate, original.respiratory_rate)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_heart_rate_measures_load as (
    insert into mc_heart_rate_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.heart_rate, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_heart_rate_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, heart_rate real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set heart_rate = coalesce(excluded.heart_rate, original.heart_rate)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_nutrients_macro_measures_load as (
    insert into mc_nutrients_macro_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.dietary_carbohydrates, JSREC.dietary_energy_consumed, JSREC.dietary_fat_total, JSREC.dietary_protein, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_nutrients_macro_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, dietary_carbohydrates real, dietary_energy_consumed real, dietary_fat_total real, dietary_protein real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set dietary_carbohydrates = coalesce(excluded.dietary_carbohydrates, original.dietary_carbohydrates), dietary_energy_consumed = coalesce(excluded.dietary_energy_consumed, original.dietary_energy_consumed), dietary_fat_total = coalesce(excluded.dietary_fat_total, original.dietary_fat_total), dietary_protein = coalesce(excluded.dietary_protein, original.dietary_protein)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_nutrients_subsets_measures_load as (
    insert into mc_nutrients_subsets_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.dietary_caffeine, JSREC.dietary_cholesterol, JSREC.dietary_fat_monounsaturated, JSREC.dietary_fat_polyunsaturated, JSREC.dietary_fat_saturated, JSREC.dietary_fiber, JSREC.dietary_sugar, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_nutrients_subsets_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, dietary_caffeine real, dietary_cholesterol real, dietary_fat_monounsaturated real, dietary_fat_polyunsaturated real, dietary_fat_saturated real, dietary_fiber real, dietary_sugar real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set dietary_caffeine = coalesce(excluded.dietary_caffeine, original.dietary_caffeine), dietary_cholesterol = coalesce(excluded.dietary_cholesterol, original.dietary_cholesterol), dietary_fat_monounsaturated = coalesce(excluded.dietary_fat_monounsaturated, original.dietary_fat_monounsaturated), dietary_fat_polyunsaturated = coalesce(excluded.dietary_fat_polyunsaturated, original.dietary_fat_polyunsaturated), dietary_fat_saturated = coalesce(excluded.dietary_fat_saturated, original.dietary_fat_saturated), dietary_fiber = coalesce(excluded.dietary_fiber, original.dietary_fiber), dietary_sugar = coalesce(excluded.dietary_sugar, original.dietary_sugar)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_nutrients_minerals_measures_load as (
    insert into mc_nutrients_minerals_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.dietary_calcium, JSREC.dietary_chloride, JSREC.dietary_chromium, JSREC.dietary_copper, JSREC.dietary_iodine, JSREC.dietary_iron, JSREC.dietary_magnesium, JSREC.dietary_manganese, JSREC.dietary_molybdenum, JSREC.dietary_niacin, JSREC.dietary_phosphorus, JSREC.dietary_potassium, JSREC.dietary_selenium, JSREC.dietary_sodium, JSREC.dietary_zinc, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_nutrients_minerals_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, dietary_calcium real, dietary_chloride real, dietary_chromium real, dietary_copper real, dietary_iodine real, dietary_iron real, dietary_magnesium real, dietary_manganese real, dietary_molybdenum real, dietary_niacin real, dietary_phosphorus real, dietary_potassium real, dietary_selenium real, dietary_sodium real, dietary_zinc real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set dietary_calcium = coalesce(excluded.dietary_calcium, original.dietary_calcium), dietary_chloride = coalesce(excluded.dietary_chloride, original.dietary_chloride), dietary_chromium = coalesce(excluded.dietary_chromium, original.dietary_chromium), dietary_copper = coalesce(excluded.dietary_copper, original.dietary_copper), dietary_iodine = coalesce(excluded.dietary_iodine, original.dietary_iodine), dietary_iron = coalesce(excluded.dietary_iron, original.dietary_iron), dietary_magnesium = coalesce(excluded.dietary_magnesium, original.dietary_magnesium), dietary_manganese = coalesce(excluded.dietary_manganese, original.dietary_manganese), dietary_molybdenum = coalesce(excluded.dietary_molybdenum, original.dietary_molybdenum), dietary_niacin = coalesce(excluded.dietary_niacin, original.dietary_niacin), dietary_phosphorus = coalesce(excluded.dietary_phosphorus, original.dietary_phosphorus), dietary_potassium = coalesce(excluded.dietary_potassium, original.dietary_potassium), dietary_selenium = coalesce(excluded.dietary_selenium, original.dietary_selenium), dietary_sodium = coalesce(excluded.dietary_sodium, original.dietary_sodium), dietary_zinc = coalesce(excluded.dietary_zinc, original.dietary_zinc)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_nutrients_vitamins_measures_load as (
    insert into mc_nutrients_vitamins_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.dietary_biotin, JSREC.dietary_folate, JSREC.dietary_pantothenic_acid, JSREC.dietary_riboflavin, JSREC.dietary_thiamin, JSREC.dietary_vitamina, JSREC.dietary_vitaminb12, JSREC.dietary_vitaminb6, JSREC.dietary_vitaminc, JSREC.dietary_vitamind, JSREC.dietary_vitamine, JSREC.dietary_vitamink, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_nutrients_vitamins_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, dietary_biotin real, dietary_folate real, dietary_pantothenic_acid real, dietary_riboflavin real, dietary_thiamin real, dietary_vitamina real, dietary_vitaminb12 real, dietary_vitaminb6 real, dietary_vitaminc real, dietary_vitamind real, dietary_vitamine real, dietary_vitamink real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set dietary_biotin = coalesce(excluded.dietary_biotin, original.dietary_biotin), dietary_folate = coalesce(excluded.dietary_folate, original.dietary_folate), dietary_pantothenic_acid = coalesce(excluded.dietary_pantothenic_acid, original.dietary_pantothenic_acid), dietary_riboflavin = coalesce(excluded.dietary_riboflavin, original.dietary_riboflavin), dietary_thiamin = coalesce(excluded.dietary_thiamin, original.dietary_thiamin), dietary_vitamina = coalesce(excluded.dietary_vitamina, original.dietary_vitamina), dietary_vitaminb12 = coalesce(excluded.dietary_vitaminb12, original.dietary_vitaminb12), dietary_vitaminb6 = coalesce(excluded.dietary_vitaminb6, original.dietary_vitaminb6), dietary_vitaminc = coalesce(excluded.dietary_vitaminc, original.dietary_vitaminc), dietary_vitamind = coalesce(excluded.dietary_vitamind, original.dietary_vitamind), dietary_vitamine = coalesce(excluded.dietary_vitamine, original.dietary_vitamine), dietary_vitamink = coalesce(excluded.dietary_vitamink, original.dietary_vitamink)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_nutrients_liquids_measures_load as (
    insert into mc_nutrients_liquids_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.dietary_alcohol, JSREC.dietary_water, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_nutrients_liquids_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, dietary_alcohol real, dietary_water real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set dietary_alcohol = coalesce(excluded.dietary_alcohol, original.dietary_alcohol), dietary_water = coalesce(excluded.dietary_water, original.dietary_water)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  mc_misc_measures_load as (
    insert into mc_misc_measures as original
    select R.sid, U.udbid, JSREC.ts, null as uuid, JSREC.apple_stand_hour, JSREC.electrodermal_activity, JSREC.nike_fuel, JSREC.number_of_times_fallen, JSREC.peripheral_perfusion_index, null as metadata
    from
      (select sid, userid, body from etl_candidates where rel = E'mc_misc_measures') R,
      lateral json_to_record(R.body::json) as JSREC(ts timestamptz, apple_stand_hour real, electrodermal_activity real, nike_fuel real, number_of_times_fallen real, peripheral_perfusion_index real),
      users U
    where decode(R.userid, 'base64') = U.id
    on conflict (sid) do
    update
      set apple_stand_hour = coalesce(excluded.apple_stand_hour, original.apple_stand_hour), electrodermal_activity = coalesce(excluded.electrodermal_activity, original.electrodermal_activity), nike_fuel = coalesce(excluded.nike_fuel, original.nike_fuel), number_of_times_fallen = coalesce(excluded.number_of_times_fallen, original.number_of_times_fallen), peripheral_perfusion_index = coalesce(excluded.peripheral_perfusion_index, original.peripheral_perfusion_index)
      where original.sid = excluded.sid and original.udbid = excluded.udbid
    returning sid
  ),
  etl_stats as (
    insert into measures_etl_jobs
    select etl_job_id, R.load_id, R.value
    from (
          select E'mc_body_measures' as load_id, count(*) as value from mc_body_measures_load
          union
          select E'mc_blood_pressure_measures' as load_id, count(*) as value from mc_blood_pressure_measures_load
          union
          select E'mc_sleep_measures' as load_id, count(*) as value from mc_sleep_measures_load
          union
          select E'mc_meal_measures' as load_id, count(*) as value from mc_meal_measures_load
          union
          select E'mc_activity_measures' as load_id, count(*) as value from mc_activity_measures_load
          union
          select E'mc_light_measures' as load_id, count(*) as value from mc_light_measures_load
          union
          select E'mc_energy_measures' as load_id, count(*) as value from mc_energy_measures_load
          union
          select E'mc_blood_measures' as load_id, count(*) as value from mc_blood_measures_load
          union
          select E'mc_lung_measures' as load_id, count(*) as value from mc_lung_measures_load
          union
          select E'mc_heart_rate_measures' as load_id, count(*) as value from mc_heart_rate_measures_load
          union
          select E'mc_nutrients_macro_measures' as load_id, count(*) as value from mc_nutrients_macro_measures_load
          union
          select E'mc_nutrients_subsets_measures' as load_id, count(*) as value from mc_nutrients_subsets_measures_load
          union
          select E'mc_nutrients_minerals_measures' as load_id, count(*) as value from mc_nutrients_minerals_measures_load
          union
          select E'mc_nutrients_vitamins_measures' as load_id, count(*) as value from mc_nutrients_vitamins_measures_load
          union
          select E'mc_nutrients_liquids_measures' as load_id, count(*) as value from mc_nutrients_liquids_measures_load
          union
          select E'mc_misc_measures' as load_id, count(*) as value from mc_misc_measures_load
    ) R
    returning job_id, load_id, value
  )
  insert into measures_etl_progress
  select etl_job_id, coalesce(R.last_measure_id, -1) as last_measure_id, (1::smallint) as dataset_type
  from (select max(sid) as last_measure_id from etl_candidates) as R;
end;
$$ language plpgsql;


create or replace function mc_etl_launch_fn() returns trigger as $$
declare
  etl_batch_size integer;
begin
  select param_value into etl_batch_size from mc_parameters where param_key = E'etl_batch_size';
  if NEW.dataset_type = 0 then
    perform measures_mc_granola_etl(NEW.job_id, etl_batch_size);
  elsif NEW.dataset_type = 1 then
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
