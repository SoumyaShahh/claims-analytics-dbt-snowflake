{{ config(materialized='table') }}

with p as (
    select * from {{ ref('int_patients_enriched') }}
),
c as (
    select * from {{ ref('int_patient_chronic_conditions') }}
)

select
    p.patient_id,
    p.first_name,
    p.last_name,
    p.birth_date,
    p.death_date,
    p.is_alive,
    p.age_years,
    p.age_band,
    p.gender,
    p.race,
    p.ethnicity,
    p.marital_status,
    p.city,
    p.state,
    p.county,
    p.zip_code,
    p.is_medicare_eligible,
    p.lifetime_healthcare_expenses,
    p.lifetime_healthcare_coverage,
    p.lifetime_out_of_pocket,

    coalesce(c.has_diabetes, 0)                   as has_diabetes,
    coalesce(c.has_hypertension, 0)               as has_hypertension,
    coalesce(c.has_heart_failure, 0)              as has_heart_failure,
    coalesce(c.has_copd, 0)                       as has_copd,
    coalesce(c.has_asthma, 0)                     as has_asthma,
    coalesce(c.has_ckd, 0)                        as has_ckd,
    coalesce(c.has_depression, 0)                 as has_depression,
    coalesce(c.has_obesity, 0)                    as has_obesity,
    coalesce(c.has_cardiovascular_disease, 0)     as has_cardiovascular_disease,
    coalesce(c.chronic_condition_count, 0)        as chronic_condition_count,
    coalesce(c.is_high_complexity_patient, false) as is_high_complexity_patient
from p
left join c on p.patient_id = c.patient_id
