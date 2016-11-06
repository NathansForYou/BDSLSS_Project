-------------------------------
-- NHanes dataset

drop table if exists nhanes_24hr_nutrients_measures;
drop table if exists nhanes_blood_pressure_measures;
drop table if exists nhanes_body_measures;
drop table if exists nhanes_demographic_measures;
drop table if exists nhanes_heart_rate_measures;
drop table if exists nhanes_sleep_measures;
drop table if exists nhanes_time_of_eating_nutrients_measures;

create table nhanes_24hr_nutrients_measures (
  unique_id                   integer not null,
  dietary_energy_consumed     integer,
  dietary_protein             real,
  dietary_carbohydrates       real,
  dietary_sugar               real,
  dietary_fiber               real,
  dietary_fat_total           real,
  dietary_fat_saturated       real,
  dietary_fat_monounsaturated real,
  dietary_fat_polyunsaturated real,
  dietary_cholesterol         integer,
  dietary_calcium             integer,
  dietary_magnesium           integer,
  dietary_iron                real,
  dietary_zinc                real,
  dietary_copper              real,
  dietary_sodium              integer,
  dietary_potassium           integer,
  dietary_caffeine            integer,
  dietary_alcohol             real,
  dietary_water               real
);

create table nhanes_blood_pressure_measures (
  unique_id                integer not null,
  systolic_blood_pressure  integer,
  diastolic_blood_pressure integer
);

create table nhanes_body_measures (
  unique_id                integer not null,
  body_weight              real,
  body_height              real,
  body_mass_index          real,
  body_mass_index_children integer,
  waist_circumference      real,
  sagittal_diameter        real
);

create table nhanes_demographic_measures (
  unique_id               integer not null,
  gender                  integer,
  age_years               integer,
  age_months              integer,
  race                    integer,
  exam_time               integer,
  education_level         integer,
  martial_status          integer,
  annual_household_income integer
);

create table nhanes_heart_rate_measures (
  unique_id  integer not null,
  heart_rate integer
);

create table nhanes_sleep_measures (
  unique_id integer not null,
  sleep     integer
);

create table nhanes_time_of_eating_nutrients_measures (
  unique_id                integer not null,
  food_type                integer,
  food_time_of_eating      time without time zone,
  food_eating_occasion     integer,
  food_source              integer,
  food_eating_location     integer,
  food_energy_consumed     integer,
  food_protein             real,
  food_carbohydrates       real,
  food_sugar               real,
  food_fiber               real,
  food_fat_food            real,
  food_fat_saturated       real,
  food_fat_monounsaturated real,
  food_fat_polyunsaturated real,
  food_cholesterol         integer,
  food_calcium             integer,
  food_magnesium           integer,
  food_iron                real,
  food_zinc                real,
  food_copper              real,
  food_sodium              integer,
  food_potassium           integer,
  food_caffeine            integer,
  food_alcohol             real
);

