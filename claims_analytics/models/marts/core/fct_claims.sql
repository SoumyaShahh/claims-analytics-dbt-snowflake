{{ config(materialized='table') }}

with c as (
    select * from {{ ref('int_claims_with_encounters') }}
)

select
    claim_id,
    patient_id,
    provider_id,
    primary_payer_id,
    secondary_payer_id,
    encounter_id,
    service_date                                     as service_date_key,
    primary_diagnosis_code,
    primary_status,
    patient_status,
    claim_payment_status,
    encounter_class,
    encounter_description,
    total_claim_cost,
    payer_coverage,
    patient_responsibility,
    payer_coverage_ratio,
    total_outstanding_amount,
    encounter_duration_minutes
from c
