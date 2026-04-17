{{ config(materialized='table') }}

-- Provider scorecard: claims volume, avg cost per claim, payer coverage ratio,
-- outstanding balance exposure, and unique-patient reach.

with claims as (
    select * from {{ ref('fct_claims') }}
),

providers as (
    select * from {{ ref('dim_providers') }}
),

agg as (
    select
        provider_id,
        count(*)                                     as total_claims,
        count(distinct patient_id)                   as unique_patients_served,
        sum(total_claim_cost)                        as total_revenue_claimed,
        avg(total_claim_cost)                        as avg_claim_cost,
        sum(payer_coverage)                          as total_payer_coverage,
        sum(patient_responsibility)                  as total_patient_responsibility,
        sum(total_outstanding_amount)                as total_outstanding,
        avg(payer_coverage_ratio)                    as avg_payer_coverage_ratio,
        sum(case when claim_payment_status = 'Outstanding' then 1 else 0 end) as outstanding_claim_count
    from claims
    group by provider_id
),

final as (
    select
        p.provider_id,
        p.provider_name,
        p.specialty,
        p.provider_state,
        p.organization_name,

        coalesce(a.total_claims, 0)                  as total_claims,
        coalesce(a.unique_patients_served, 0)        as unique_patients_served,
        coalesce(a.total_revenue_claimed, 0)         as total_revenue_claimed,
        coalesce(a.avg_claim_cost, 0)                as avg_claim_cost,
        coalesce(a.total_payer_coverage, 0)          as total_payer_coverage,
        coalesce(a.total_patient_responsibility, 0)  as total_patient_responsibility,
        coalesce(a.total_outstanding, 0)             as total_outstanding,
        coalesce(a.avg_payer_coverage_ratio, 0)      as avg_payer_coverage_ratio,
        coalesce(a.outstanding_claim_count, 0)       as outstanding_claim_count,

        case
            when coalesce(a.total_claims, 0) = 0 then 0
            else coalesce(a.outstanding_claim_count, 0) * 1.0 / a.total_claims
        end                                          as outstanding_claim_rate
    from providers p
    left join agg a on p.provider_id = a.provider_id
)

select * from final