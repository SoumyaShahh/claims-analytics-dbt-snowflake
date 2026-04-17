{{ config(materialized='view') }}

with source as (
    select * from {{ source('synthea_raw', 'raw_payers') }}
),

renamed as (
    select
        id                          as payer_id,
        name                        as payer_name,
        address,
        city,
        state_headquartered         as headquarters_state,
        zip                         as zip_code,
        phone,
        amount_covered              as total_amount_covered,
        amount_uncovered            as total_amount_uncovered,
        revenue                     as total_revenue,
        covered_encounters,
        uncovered_encounters,
        covered_medications,
        uncovered_medications,
        covered_procedures,
        uncovered_procedures,
        covered_immunizations,
        uncovered_immunizations,
        unique_customers            as unique_member_count,
        qols_avg                    as avg_quality_of_life_score,
        member_months
    from source
)

select * from renamed