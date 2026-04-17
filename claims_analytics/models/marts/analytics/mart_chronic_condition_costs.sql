{{ config(materialized='table') }}

-- Cost burden by chronic condition. Shows avg and total spend per condition,
-- which is critical payer-side analytics (where are claim dollars going?).

with patients as (
    select * from {{ ref('dim_patients') }}
),

claim_costs as (
    select
        patient_id,
        sum(total_claim_cost)                        as total_billed_amount,
        sum(payer_coverage)                          as total_payer_coverage,
        count(*)                                     as total_claims
    from {{ ref('fct_claims') }}
    group by patient_id
),

joined as (
    select
        p.patient_id,
        p.has_diabetes,
        p.has_hypertension,
        p.has_heart_failure,
        p.has_copd,
        p.has_asthma,
        p.has_ckd,
        p.has_depression,
        p.has_obesity,
        p.has_cardiovascular_disease,
        coalesce(c.total_billed_amount, 0)          as total_billed_amount,
        coalesce(c.total_payer_coverage, 0)         as total_payer_coverage,
        coalesce(c.total_claims, 0)                 as total_claims
    from patients p
    left join claim_costs c on p.patient_id = c.patient_id
),

unpivoted as (
    select 'Diabetes'              as condition, has_diabetes              as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Hypertension'          as condition, has_hypertension          as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Heart Failure'         as condition, has_heart_failure         as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'COPD'                  as condition, has_copd                  as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Asthma'                as condition, has_asthma                as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Chronic Kidney Disease' as condition, has_ckd                  as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Depression'            as condition, has_depression            as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Obesity'               as condition, has_obesity               as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
    union all
    select 'Cardiovascular Disease' as condition, has_cardiovascular_disease as has_condition, total_billed_amount, total_payer_coverage, total_claims from joined
),

final as (
    select
        condition,
        count(*)                                     as patients_with_condition,
        sum(total_billed_amount)                     as total_billed_for_cohort,
        sum(total_payer_coverage)                    as total_payer_coverage_for_cohort,
        sum(total_claims)                            as total_claims_for_cohort,
        avg(total_billed_amount)                     as avg_billed_per_patient,
        avg(total_claims)                            as avg_claims_per_patient
    from unpivoted
    where has_condition = 1
    group by condition
)

select * from final
order by total_billed_for_cohort desc