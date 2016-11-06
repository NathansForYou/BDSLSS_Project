--
-- Data reset
-------------------
create or replace function reset_measures() returns void as
$$
begin
  truncate table mc_granola_measures;
  truncate table mc_json_measures;
  truncate table mc_body_measures;
  truncate table mc_blood_pressure_measures;
  truncate table mc_sleep_measures;
  truncate table mc_meal_measures;
  truncate table mc_activity_measures;
  truncate table mc_light_measures;
  truncate table mc_energy_measures;
  truncate table mc_blood_measures;
  truncate table mc_lung_measures;
  truncate table mc_heart_rate_measures;
  truncate table mc_nutrients_macro_measures;
  truncate table mc_nutrients_subsets_measures;
  truncate table mc_nutrients_minerals_measures;
  truncate table mc_nutrients_vitamins_measures;
  truncate table mc_nutrients_liquids_measures;
  truncate table mc_misc_measures;
  return;
end
$$
language plpgsql;

create or replace function reset_measure_views() returns void as
$$
begin
  truncate table mc_sumcount_by_user;
  truncate table mc_sumcount_by_user_day;
  truncate table mc_vm_user_windows;
  return;
end
$$
language plpgsql;

--
-- Task reset
------------------------------
create or replace function reset_measure_etl() returns void as
$$
begin
  truncate table measures_etl_launch;
  truncate table measures_etl_jobs;
  truncate table measures_etl_progress;
  return;
end
$$
language plpgsql;

create or replace function reset_measure_gc() returns void as
$$
begin
  truncate table measures_gc_launch;
  truncate table measures_gc_progress;
  return;
end
$$
language plpgsql;

create or replace function reset_measure_tasks() returns void as
$$
begin
  perform reset_measure_etl();
  perform reset_measure_gc();
  return;
end
$$
language plpgsql;

create or replace function reset_measure_tasks_with_seq() returns void as
$$
begin
  perform reset_measure_tasks();
  perform setval('measures_etl_launch_job_id_seq', 1, false);
  perform setval('measures_gc_launch_job_id_seq', 1, false);
  return;
end
$$
language plpgsql;


--
-- User reset
----------------
create or replace function reset_all_user_profiles() returns void as
$$
begin
  update users
    set profile=('{ "age": "24", "sex": "male", "metric": "false", "nike_fuel": 0, "heart_rate": 0, "body_height": 0, "body_weight": 0, "uv_exposure": 0, "dietary_iron": 0, "dietary_zinc": 0, "blood_glucose": 0, "dietary_fiber": 0, "dietary_sugar": 0, "dietary_water": 0, "inhaler_usage": 0, "meal_duration": { "lunch": 0, "snack": 0, "dinner": 0, "breakfast": 0 }, "activity_value": { "golf": { "distance": 0, "kcal_burned": 0 }, "play": { "distance": 0, "kcal_burned": 0 }, "yoga": { "distance": 0, "kcal_burned": 0 }, "dance": { "distance": 0, "kcal_burned": 0 }, "other": { "distance": 0, "kcal_burned": 0 }, "rugby": { "distance": 0, "kcal_burned": 0 }, "boxing": { "distance": 0, "kcal_burned": 0 }, "hiking": { "distance": 0, "kcal_burned": 0 }, "hockey": { "distance": 0, "kcal_burned": 0 }, "rowing": { "distance": 0, "kcal_burned": 0 }, "soccer": { "distance": 0, "kcal_burned": 0 }, "squash": { "distance": 0, "kcal_burned": 0 }, "tennis": { "distance": 0, "kcal_burned": 0 }, "archery": { "distance": 0, "kcal_burned": 0 }, "bowling": { "distance": 0, "kcal_burned": 0 }, "cricket": { "distance": 0, "kcal_burned": 0 }, "curling": { "distance": 0, "kcal_burned": 0 }, "cycling": { "distance": 0, "kcal_burned": 0 }, "fencing": { "distance": 0, "kcal_burned": 0 }, "fishing": { "distance": 0, "kcal_burned": 0 }, "hunting": { "distance": 0, "kcal_burned": 0 }, "running": { "distance": 0, "kcal_burned": 0 }, "sailing": { "distance": 0, "kcal_burned": 0 }, "walking": { "distance": 0, "kcal_burned": 0 }, "baseball": { "distance": 0, "kcal_burned": 0 }, "climbing": { "distance": 0, "kcal_burned": 0 }, "handball": { "distance": 0, "kcal_burned": 0 }, "lacrosse": { "distance": 0, "kcal_burned": 0 }, "softball": { "distance": 0, "kcal_burned": 0 }, "swimming": { "distance": 0, "kcal_burned": 0 }, "badminton": { "distance": 0, "kcal_burned": 0 }, "wrestling": { "distance": 0, "kcal_burned": 0 }, "basketball": { "distance": 0, "kcal_burned": 0 }, "elliptical": { "distance": 0, "kcal_burned": 0 }, "gymnastics": { "distance": 0, "kcal_burned": 0 }, "step_count": { "step_count": 0 }, "volleyball": { "distance": 0, "kcal_burned": 0 }, "water_polo": { "distance": 0, "kcal_burned": 0 }, "racquetball": { "distance": 0, "kcal_burned": 0 }, "snow_sports": { "distance": 0, "kcal_burned": 0 }, "martial_arts": { "distance": 0, "kcal_burned": 0 }, "table_tennis": { "distance": 0, "kcal_burned": 0 }, "water_sports": { "distance": 0, "kcal_burned": 0 }, "mind_and_body": { "distance": 0, "kcal_burned": 0 }, "paddle_sports": { "distance": 0, "kcal_burned": 0 }, "water_fitness": { "distance": 0, "kcal_burned": 0 }, "cross_training": { "distance": 0, "kcal_burned": 0 }, "skating_sports": { "distance": 0, "kcal_burned": 0 }, "stair_climbing": { "distance": 0, "kcal_burned": 0 }, "surfing_sports": { "distance": 0, "kcal_burned": 0 }, "flights_climbed": { "flights": 0 }, "track_and_field": { "distance": 0, "kcal_burned": 0 }, "american_football": { "distance": 0, "kcal_burned": 0 }, "equestrian_sports": { "distance": 0, "kcal_burned": 0 }, "australian_football": { "distance": 0, "kcal_burned": 0 }, "dance_inspired_training": { "distance": 0, "kcal_burned": 0 }, "distance_walking_running": { "distance": 0 }, "preparation_and_recovery": { "distance": 0, "kcal_burned": 0 }, "functional_strength_training": { "distance": 0, "kcal_burned": 0 }, "traditional_strength_training": { "distance": 0, "kcal_burned": 0 }, "mixed_metabolic_cardio_training": { "distance": 0, "kcal_burned": 0 } }, "dietary_biotin": 0, "dietary_copper": 0, "dietary_folate": 0, "dietary_iodine": 0, "dietary_niacin": 0, "dietary_sodium": 0, "lean_body_mass": 0, "sleep_duration": 6, "body_mass_index": 0, "dietary_alcohol": 0, "dietary_calcium": 0, "dietary_protein": 0, "dietary_thiamin": 0, "apple_stand_hour": 0, "body_temperature": 0, "dietary_caffeine": 0, "dietary_chloride": 0, "dietary_chromium": 0, "dietary_selenium": 0, "dietary_vitamina": 0, "dietary_vitaminc": 0, "dietary_vitamind": 0, "dietary_vitamine": 0, "dietary_vitamink": 0, "respiratory_rate": 0, "activity_duration": { "golf": 0, "play": 0, "yoga": 0, "dance": 0, "other": 0, "rugby": 0, "boxing": 0, "hiking": 0, "hockey": 0, "rowing": 0, "soccer": 0, "squash": 0, "tennis": 0, "archery": 0, "bowling": 0, "cricket": 0, "curling": 0, "cycling": 0, "fencing": 0, "fishing": 0, "hunting": 0, "running": 0, "sailing": 0, "walking": 0, "baseball": 0, "climbing": 0, "handball": 0, "lacrosse": 0, "softball": 0, "swimming": 0, "badminton": 0, "wrestling": 0, "basketball": 0, "elliptical": 0, "gymnastics": 0, "step_count": 0, "volleyball": 0, "water_polo": 0, "racquetball": 0, "snow_sports": 0, "martial_arts": 0, "table_tennis": 0, "water_sports": 0, "mind_and_body": 0, "paddle_sports": 0, "water_fitness": 0, "cross_training": 0, "skating_sports": 0, "stair_climbing": 0, "surfing_sports": 0, "flights_climbed": 0, "track_and_field": 0, "american_football": 0, "equestrian_sports": 0, "australian_football": 0, "dance_inspired_training": 0, "distance_walking_running": 0, "preparation_and_recovery": 0, "functional_strength_training": 0, "traditional_strength_training": 0, "mixed_metabolic_cardio_training": 0 }, "dietary_fat_total": 0, "dietary_magnesium": 0, "dietary_manganese": 0, "dietary_potassium": 0, "dietary_vitaminb6": 0, "dietary_molybdenum": 0, "dietary_phosphorus": 0, "dietary_riboflavin": 0, "dietary_vitaminb12": 0, "basal_energy_burned": 0, "body_fat_percentage": 0, "dietary_cholesterol": 0, "active_energy_burned": 0, "peak_expiratory_flow": 0, "blood_alcohol_content": 0, "dietary_carbohydrates": 0, "dietary_fat_saturated": 0, "forced_vital_capacity": 0, "basal_body_temperature": 0, "electrodermal_activity": 0, "number_of_times_fallen": 0, "blood_oxygen_saturation": 0, "dietary_energy_consumed": 0, "systolic_blood_pressure": 0, "diastolic_blood_pressure": 0, "dietary_pantothenic_acid": 0, "peripheral_perfusion_index": 0, "dietary_fat_monounsaturated": 0, "dietary_fat_polyunsaturated": 0, "forced_expiratory_volume_one_second": 0  }')::jsonb;
  return;
end
$$
language plpgsql;

create or replace function reset_user_profile(encoded_user_id text) returns void as
$$
begin
  update users
    set profile=('{ "age": "24", "sex": "male", "metric": "false", "nike_fuel": 0, "heart_rate": 0, "body_height": 0, "body_weight": 0, "uv_exposure": 0, "dietary_iron": 0, "dietary_zinc": 0, "blood_glucose": 0, "dietary_fiber": 0, "dietary_sugar": 0, "dietary_water": 0, "inhaler_usage": 0, "meal_duration": { "lunch": 0, "snack": 0, "dinner": 0, "breakfast": 0 }, "activity_value": { "golf": { "distance": 0, "kcal_burned": 0 }, "play": { "distance": 0, "kcal_burned": 0 }, "yoga": { "distance": 0, "kcal_burned": 0 }, "dance": { "distance": 0, "kcal_burned": 0 }, "other": { "distance": 0, "kcal_burned": 0 }, "rugby": { "distance": 0, "kcal_burned": 0 }, "boxing": { "distance": 0, "kcal_burned": 0 }, "hiking": { "distance": 0, "kcal_burned": 0 }, "hockey": { "distance": 0, "kcal_burned": 0 }, "rowing": { "distance": 0, "kcal_burned": 0 }, "soccer": { "distance": 0, "kcal_burned": 0 }, "squash": { "distance": 0, "kcal_burned": 0 }, "tennis": { "distance": 0, "kcal_burned": 0 }, "archery": { "distance": 0, "kcal_burned": 0 }, "bowling": { "distance": 0, "kcal_burned": 0 }, "cricket": { "distance": 0, "kcal_burned": 0 }, "curling": { "distance": 0, "kcal_burned": 0 }, "cycling": { "distance": 0, "kcal_burned": 0 }, "fencing": { "distance": 0, "kcal_burned": 0 }, "fishing": { "distance": 0, "kcal_burned": 0 }, "hunting": { "distance": 0, "kcal_burned": 0 }, "running": { "distance": 0, "kcal_burned": 0 }, "sailing": { "distance": 0, "kcal_burned": 0 }, "walking": { "distance": 0, "kcal_burned": 0 }, "baseball": { "distance": 0, "kcal_burned": 0 }, "climbing": { "distance": 0, "kcal_burned": 0 }, "handball": { "distance": 0, "kcal_burned": 0 }, "lacrosse": { "distance": 0, "kcal_burned": 0 }, "softball": { "distance": 0, "kcal_burned": 0 }, "swimming": { "distance": 0, "kcal_burned": 0 }, "badminton": { "distance": 0, "kcal_burned": 0 }, "wrestling": { "distance": 0, "kcal_burned": 0 }, "basketball": { "distance": 0, "kcal_burned": 0 }, "elliptical": { "distance": 0, "kcal_burned": 0 }, "gymnastics": { "distance": 0, "kcal_burned": 0 }, "step_count": { "step_count": 0 }, "volleyball": { "distance": 0, "kcal_burned": 0 }, "water_polo": { "distance": 0, "kcal_burned": 0 }, "racquetball": { "distance": 0, "kcal_burned": 0 }, "snow_sports": { "distance": 0, "kcal_burned": 0 }, "martial_arts": { "distance": 0, "kcal_burned": 0 }, "table_tennis": { "distance": 0, "kcal_burned": 0 }, "water_sports": { "distance": 0, "kcal_burned": 0 }, "mind_and_body": { "distance": 0, "kcal_burned": 0 }, "paddle_sports": { "distance": 0, "kcal_burned": 0 }, "water_fitness": { "distance": 0, "kcal_burned": 0 }, "cross_training": { "distance": 0, "kcal_burned": 0 }, "skating_sports": { "distance": 0, "kcal_burned": 0 }, "stair_climbing": { "distance": 0, "kcal_burned": 0 }, "surfing_sports": { "distance": 0, "kcal_burned": 0 }, "flights_climbed": { "flights": 0 }, "track_and_field": { "distance": 0, "kcal_burned": 0 }, "american_football": { "distance": 0, "kcal_burned": 0 }, "equestrian_sports": { "distance": 0, "kcal_burned": 0 }, "australian_football": { "distance": 0, "kcal_burned": 0 }, "dance_inspired_training": { "distance": 0, "kcal_burned": 0 }, "distance_walking_running": { "distance": 0 }, "preparation_and_recovery": { "distance": 0, "kcal_burned": 0 }, "functional_strength_training": { "distance": 0, "kcal_burned": 0 }, "traditional_strength_training": { "distance": 0, "kcal_burned": 0 }, "mixed_metabolic_cardio_training": { "distance": 0, "kcal_burned": 0 } }, "dietary_biotin": 0, "dietary_copper": 0, "dietary_folate": 0, "dietary_iodine": 0, "dietary_niacin": 0, "dietary_sodium": 0, "lean_body_mass": 0, "sleep_duration": 6, "body_mass_index": 0, "dietary_alcohol": 0, "dietary_calcium": 0, "dietary_protein": 0, "dietary_thiamin": 0, "apple_stand_hour": 0, "body_temperature": 0, "dietary_caffeine": 0, "dietary_chloride": 0, "dietary_chromium": 0, "dietary_selenium": 0, "dietary_vitamina": 0, "dietary_vitaminc": 0, "dietary_vitamind": 0, "dietary_vitamine": 0, "dietary_vitamink": 0, "respiratory_rate": 0, "activity_duration": { "golf": 0, "play": 0, "yoga": 0, "dance": 0, "other": 0, "rugby": 0, "boxing": 0, "hiking": 0, "hockey": 0, "rowing": 0, "soccer": 0, "squash": 0, "tennis": 0, "archery": 0, "bowling": 0, "cricket": 0, "curling": 0, "cycling": 0, "fencing": 0, "fishing": 0, "hunting": 0, "running": 0, "sailing": 0, "walking": 0, "baseball": 0, "climbing": 0, "handball": 0, "lacrosse": 0, "softball": 0, "swimming": 0, "badminton": 0, "wrestling": 0, "basketball": 0, "elliptical": 0, "gymnastics": 0, "step_count": 0, "volleyball": 0, "water_polo": 0, "racquetball": 0, "snow_sports": 0, "martial_arts": 0, "table_tennis": 0, "water_sports": 0, "mind_and_body": 0, "paddle_sports": 0, "water_fitness": 0, "cross_training": 0, "skating_sports": 0, "stair_climbing": 0, "surfing_sports": 0, "flights_climbed": 0, "track_and_field": 0, "american_football": 0, "equestrian_sports": 0, "australian_football": 0, "dance_inspired_training": 0, "distance_walking_running": 0, "preparation_and_recovery": 0, "functional_strength_training": 0, "traditional_strength_training": 0, "mixed_metabolic_cardio_training": 0 }, "dietary_fat_total": 0, "dietary_magnesium": 0, "dietary_manganese": 0, "dietary_potassium": 0, "dietary_vitaminb6": 0, "dietary_molybdenum": 0, "dietary_phosphorus": 0, "dietary_riboflavin": 0, "dietary_vitaminb12": 0, "basal_energy_burned": 0, "body_fat_percentage": 0, "dietary_cholesterol": 0, "active_energy_burned": 0, "peak_expiratory_flow": 0, "blood_alcohol_content": 0, "dietary_carbohydrates": 0, "dietary_fat_saturated": 0, "forced_vital_capacity": 0, "basal_body_temperature": 0, "electrodermal_activity": 0, "number_of_times_fallen": 0, "blood_oxygen_saturation": 0, "dietary_energy_consumed": 0, "systolic_blood_pressure": 0, "diastolic_blood_pressure": 0, "dietary_pantothenic_acid": 0, "peripheral_perfusion_index": 0, "dietary_fat_monounsaturated": 0, "dietary_fat_polyunsaturated": 0, "forced_expiratory_volume_one_second": 0  }')::jsonb
    where encode(id, 'base64') = encoded_user_id;
  return;
end
$$
language plpgsql;

create or replace function reset_all_users_sync() returns void as
$$
begin
  update users
    set last_acquired=('{}')::jsonb, archive_span=('{"min_ts": {}, "start_ts": {}, "end_ts": {}}')::jsonb;
  return;
end
$$
language plpgsql;


create or replace function reset_user_sync(encoded_user_id text) returns void as
$$
begin
  update users
    set last_acquired=('{}')::jsonb, archive_span=('{"min_ts": {}, "start_ts": {}, "end_ts": {}}')::jsonb
    where encode(id, 'base64') = encoded_user_id;
  return;
end
$$
language plpgsql;

create or replace function reset_user(encoded_user_id text) returns void as
$$
begin
  perform reset_user_profile(encoded_user_id);
  perform reset_user_sync(encoded_user_id);
end
$$
language plpgsql;


--
-- Whole database reset
-------------------------
create or replace function reset_db_data() returns void as
$$
begin
  perform reset_measures();
  perform reset_measure_views();
  perform reset_measure_tasks_with_seq();
  return;
end
$$
language plpgsql;

create or replace function reset_database() returns void as
$$
begin
  perform reset_db_data();
  perform reset_all_users_sync();
  return;
end
$$
language plpgsql;

