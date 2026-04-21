{{ config(materialized='view') }}

-- kept as a view (not ephemeral) because snowflake's column inference
-- was dropping has_* flags when the max(case when) returned no rows.

with cond as (
    select * from {{ ref('stg_conditions') }}
),

flags as (
    select
        patient_id,
        max(case when lower(condition_description) like '%diabetes%' then 1 else 0 end) as has_diabetes,
        max(case when lower(condition_description) like '%hypertension%'
              or lower(condition_description) like '%high blood pressure%' then 1 else 0 end) as has_hypertension,
        max(case when lower(condition_description) like '%heart failure%'
              or lower(condition_description) like '%chf%' then 1 else 0 end) as has_heart_failure,
        max(case when lower(condition_description) like '%copd%'
              or lower(condition_description) like '%chronic obstructive%' then 1 else 0 end) as has_copd,
        max(case when lower(condition_description) like '%asthma%' then 1 else 0 end) as has_asthma,
        max(case when lower(condition_description) like '%chronic kidney%'
              or lower(condition_description) like '%ckd%' then 1 else 0 end) as has_ckd,
        max(case when lower(condition_description) like '%depression%' then 1 else 0 end) as has_depression,
        max(case when lower(condition_description) like '%obesity%' then 1 else 0 end) as has_obesity,
        max(case when lower(condition_description) like '%coronary%'
              or lower(condition_description) like '%cardiovascular%' then 1 else 0 end) as has_cardiovascular_disease,
        count(distinct condition_code) as total_distinct_conditions
    from cond
    group by patient_id
)

select
    patient_id,
    has_diabetes,
    has_hypertension,
    has_heart_failure,
    has_copd,
    has_asthma,
    has_ckd,
    has_depression,
    has_obesity,
    has_cardiovascular_disease,
    total_distinct_conditions,
    (has_diabetes + has_hypertension + has_heart_failure
     + has_copd + has_asthma + has_ckd
     + has_depression + has_obesity + has_cardiovascular_disease) as chronic_condition_count,
    case
        when (has_diabetes + has_hypertension + has_heart_failure
              + has_copd + has_asthma + has_ckd
              + has_depression + has_obesity + has_cardiovascular_disease) >= 3
        then true else false
    end as is_high_complexity_patient
from flags
