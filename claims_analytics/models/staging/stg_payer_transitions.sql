{{ config(materialized='view') }}

with src as (
    select * from {{ source('synthea_raw', 'raw_payer_transitions') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['patient', 'memberid', 'payer', 'start_year']) }} as payer_transition_id,
    patient                          as patient_id,
    memberid                         as member_id,
    cast(start_year as date)         as coverage_start_date,
    cast(end_year as date)           as coverage_end_date,
    payer                            as payer_id,
    secondary_payer                  as secondary_payer_id,
    ownership                        as plan_ownership,
    ownername                        as plan_owner_name,
    case when end_year is null or end_year > current_date() then true else false end as is_current_coverage
from src
