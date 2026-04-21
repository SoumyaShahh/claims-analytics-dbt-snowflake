{{ config(materialized='table') }}

with e as (
    select * from {{ ref('stg_encounters') }}
)

select
    encounter_id,
    patient_id,
    provider_id,
    organization_id,
    payer_id,
    encounter_date                                   as service_date_key,
    encounter_class,
    encounter_code,
    encounter_description,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    patient_responsibility,
    payer_coverage_ratio,
    encounter_duration_minutes,
    reason_code,
    reason_description
from e
