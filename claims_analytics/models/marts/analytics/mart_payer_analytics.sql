{{ config(materialized='table') }}

with enc as (
    select * from {{ ref('fct_encounters') }}
),
pay as (
    select * from {{ ref('dim_payers') }}
),

agg as (
    select
        payer_id,
        count(*)                                     as total_encounters,
        count(distinct patient_id)                   as unique_members_seen,
        sum(total_claim_cost)                        as total_encounter_cost,
        sum(payer_coverage)                          as total_amount_covered_encounters,
        sum(patient_responsibility)                  as total_member_out_of_pocket,
        avg(payer_coverage_ratio)                    as avg_coverage_ratio,

        sum(case when encounter_class = 'inpatient'  then 1 else 0 end) as inpatient_encounters,
        sum(case when encounter_class = 'outpatient' then 1 else 0 end) as outpatient_encounters,
        sum(case when encounter_class = 'emergency'  then 1 else 0 end) as emergency_encounters,
        sum(case when encounter_class = 'wellness'   then 1 else 0 end) as wellness_encounters,
        sum(case when encounter_class = 'urgentcare' then 1 else 0 end) as urgentcare_encounters,
        sum(case when encounter_class = 'ambulatory' then 1 else 0 end) as ambulatory_encounters
    from enc
    group by payer_id
)

select
    p.payer_id,
    p.payer_name,
    p.headquarters_state,
    p.unique_member_count                        as enrolled_member_count,
    p.member_months,
    p.total_revenue                              as payer_total_revenue,
    p.coverage_ratio                             as payer_lifetime_coverage_ratio,

    coalesce(a.total_encounters, 0)              as total_encounters,
    coalesce(a.unique_members_seen, 0)           as unique_members_seen,
    coalesce(a.total_encounter_cost, 0)          as total_encounter_cost,
    coalesce(a.total_amount_covered_encounters, 0) as total_amount_covered_encounters,
    coalesce(a.total_member_out_of_pocket, 0)    as total_member_out_of_pocket,
    coalesce(a.avg_coverage_ratio, 0)            as avg_encounter_coverage_ratio,

    coalesce(a.inpatient_encounters, 0)          as inpatient_encounters,
    coalesce(a.outpatient_encounters, 0)         as outpatient_encounters,
    coalesce(a.emergency_encounters, 0)          as emergency_encounters,
    coalesce(a.wellness_encounters, 0)           as wellness_encounters,
    coalesce(a.urgentcare_encounters, 0)         as urgentcare_encounters,
    coalesce(a.ambulatory_encounters, 0)         as ambulatory_encounters
from pay p
left join agg a on p.payer_id = a.payer_id
