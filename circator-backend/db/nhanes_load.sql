----------------------------------------
-- Import NHANES data

drop table if exists tmp_nhanes_body_measures;
drop table if exists tmp_nhanes_sleep_measures;

create temporary table tmp_nhanes_body_measures (
	like nhanes_body_measures
);

create temporary table tmp_nhanes_sleep_measures (
	like nhanes_sleep_measures
);

\copy tmp_nhanes_body_measures                 from '@@PATH@@/1999-2000/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2001-2002/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2003-2004/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2005-2006/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2007-2008/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2009-2010/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2011-2012/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_body_measures                 from '@@PATH@@/2013-2014/nhanes_body_measures.csv'           with DELIMITER ',' NULL 'NA';

\copy tmp_nhanes_sleep_measures                from '@@PATH@@/2005-2006/nhanes_sleep_measures.csv'          with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_sleep_measures                from '@@PATH@@/2007-2008/nhanes_sleep_measures.csv'          with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_sleep_measures                from '@@PATH@@/2009-2010/nhanes_sleep_measures.csv'          with DELIMITER ',' NULL 'NA';
\copy tmp_nhanes_sleep_measures                from '@@PATH@@/2011-2012/nhanes_sleep_measures.csv'          with DELIMITER ',' NULL 'NA';


----------------------------------------------------------
-- Convert body_weight into lbs and sleep into seconds.
--
insert into nhanes_body_measures
select   unique_id                as unique_id,
         (body_weight * 2.20462)  as body_weight,
         body_height              as body_height,
         body_mass_index          as body_mass_index,
         body_mass_index_children as body_mass_index_children,
         waist_circumference      as waist_circumference,
         sagittal_diameter        as sagittal_diameter
from tmp_nhanes_body_measures;

drop table tmp_nhanes_body_measures;

insert into nhanes_sleep_measures
select   unique_id        as unique_id,
         (sleep * 3600.0) as sleep
from tmp_nhanes_sleep_measures;

drop table tmp_nhanes_sleep_measures;

\copy nhanes_blood_pressure_measures           from '@@PATH@@/1999-2000/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2001-2002/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2003-2004/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2005-2006/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2007-2008/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2009-2010/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2011-2012/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_blood_pressure_measures           from '@@PATH@@/2013-2014/nhanes_blood_pressure_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/1999-2000/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2001-2002/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2003-2004/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2005-2006/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2007-2008/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2009-2010/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2011-2012/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_demographic_measures              from '@@PATH@@/2013-2014/nhanes_demographics.csv'            with DELIMITER ',' NULL 'NA';
\copy nhanes_24hr_nutrients_measures           from '@@PATH@@/2003-2004/nhanes_total_24hr_nutrients_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_24hr_nutrients_measures           from '@@PATH@@/2005-2006/nhanes_total_24hr_nutrients_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_24hr_nutrients_measures           from '@@PATH@@/2007-2008/nhanes_total_24hr_nutrients_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_24hr_nutrients_measures           from '@@PATH@@/2009-2010/nhanes_total_24hr_nutrients_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_24hr_nutrients_measures           from '@@PATH@@/2011-2012/nhanes_total_24hr_nutrients_measures.csv' with DELIMITER ',' NULL 'NA';
\copy nhanes_heart_rate_measures               from '@@PATH@@/1999-2000/nhanes_heart_rate_measures.csv'     with DELIMITER ',' NULL 'NA';
\copy nhanes_heart_rate_measures               from '@@PATH@@/2001-2002/nhanes_heart_rate_measures.csv'     with DELIMITER ',' NULL 'NA';
\copy nhanes_heart_rate_measures               from '@@PATH@@/2003-2004/nhanes_heart_rate_measures.csv'     with DELIMITER ',' NULL 'NA';

-- Mapped schema loading
\copy nhanes_24hr_nutrients_measures ( unique_id, dietary_energy_consumed, dietary_protein, dietary_carbohydrates, dietary_sugar, dietary_fiber, dietary_fat_total, dietary_fat_saturated, dietary_fat_monounsaturated, dietary_fat_polyunsaturated, dietary_cholesterol, dietary_calcium, dietary_magnesium, dietary_iron, dietary_zinc, dietary_sodium, dietary_potassium, dietary_caffeine, dietary_alcohol, dietary_water) from '@@PATH@@/2001-2002/nhanes_total_24hr_nutrients_measures.csv' with DELIMITER ',' NULL 'NA';
