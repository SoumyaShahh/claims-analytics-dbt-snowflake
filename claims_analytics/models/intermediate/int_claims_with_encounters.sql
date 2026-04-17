{{ config(materialized='ephemeral') }}

with claims as (
    select * from {{ ref('stg_claims') }}
),

encounters as (
    select * from {{ ref('stg_encounters') }}
),

joined as (
    select
        c.claim_id,
        c.patient_id,
        c.provider_id,
        c.primary_payer_id,
        c.secondary_payer_id,
        c.encounter_id,
        c.service_date,
        c.primary_diagnosis_code,
        c.diagnosis_code_2,
        c.diagnosis_code_3,
        c.primary_status,
        c.patient_status,
        c.total_outstanding_amount,

        e.encounter_class,
        e.encounter_description,
        e.total_claim_cost,
        e.payer_coverage,
        e.patient_responsibility,
        e.payer_coverage_ratio,
        e.encounter_date,
        e.encounter_duration_minutes,

        case
            when c.total_outstanding_amount = 0 and c.patient_status = 'CLOSED' then 'Paid'
            when c.total_outstanding_amount > 0 and c.patient_status = 'CLOSED' then 'Partially Paid'
            when c.total_outstanding_amount > 0 then 'Outstanding'
            else 'Unknown'
        end as claim_payment_status

    from claims c
    left join encounters e on c.encounter_id = e.encounter_id
)

select * from joined