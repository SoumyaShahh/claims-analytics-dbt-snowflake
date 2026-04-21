{{ config(materialized='table') }}

with claims as (
    select * from {{ ref('fct_claims') }}
),
p as (
    select * from {{ ref('dim_patients') }}
),

agg as (
    select
        patient_id,
        count(*)                                     as total_claims,
        sum(total_claim_cost)                        as total_billed_amount,
        sum(payer_coverage)                          as total_payer_coverage,
        sum(patient_responsibility)                  as total_patient_responsibility,
        sum(total_outstanding_amount)                as total_outstanding,
        avg(total_claim_cost)                        as avg_claim_cost,
        max(total_claim_cost)                        as max_single_claim_cost,
        sum(case when claim_payment_status = 'Paid' then 1 else 0 end) as paid_claim_count,
        sum(case when claim_payment_status = 'Outstanding' then 1 else 0 end) as outstanding_claim_count
    from claims
    group by patient_id
)

select
    p.patient_id,
    p.first_name,
    p.last_name,
    p.age_years,
    p.age_band,
    p.gender,
    p.state,
    p.is_medicare_eligible,
    p.chronic_condition_count,
    p.is_high_complexity_patient,

    coalesce(a.total_claims, 0)                  as total_claims,
    coalesce(a.total_billed_amount, 0)           as total_billed_amount,
    coalesce(a.total_payer_coverage, 0)          as total_payer_coverage,
    coalesce(a.total_patient_responsibility, 0)  as total_patient_responsibility,
    coalesce(a.total_outstanding, 0)             as total_outstanding,
    coalesce(a.avg_claim_cost, 0)                as avg_claim_cost,
    coalesce(a.max_single_claim_cost, 0)         as max_single_claim_cost,
    coalesce(a.paid_claim_count, 0)              as paid_claim_count,
    coalesce(a.outstanding_claim_count, 0)       as outstanding_claim_count,

    ntile(10) over (order by coalesce(a.total_billed_amount, 0) desc) as cost_decile,
    case
        when ntile(10) over (order by coalesce(a.total_billed_amount, 0) desc) = 1 then true
        else false
    end as is_top_decile_spender
from p
left join agg a on p.patient_id = a.patient_id
