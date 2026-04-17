{{ config(materialized='table') }}

with payers as (
    select * from {{ ref('stg_payers') }}
),

final as (
    select
        payer_id,
        payer_name,
        headquarters_state,
        city                                        as payer_city,
        unique_member_count,
        member_months,
        total_revenue,
        total_amount_covered,
        total_amount_uncovered,
        case
            when (total_amount_covered + total_amount_uncovered) > 0
              then total_amount_covered / (total_amount_covered + total_amount_uncovered)
            else 0
        end                                         as coverage_ratio,
        covered_encounters,
        uncovered_encounters,
        covered_medications,
        uncovered_medications,
        covered_procedures,
        uncovered_procedures,
        avg_quality_of_life_score
    from payers
)

select * from final