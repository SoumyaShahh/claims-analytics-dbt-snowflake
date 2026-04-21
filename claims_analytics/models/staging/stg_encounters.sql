{{ config(materialized='view') }}

-- start/stop are reserved words in snowflake so they stay quoted

with src as (
    select * from {{ source('synthea_raw', 'raw_encounters') }}
)

select
    id                                                       as encounter_id,
    "START"                                                  as encounter_start_at,
    "STOP"                                                   as encounter_end_at,
    cast("START" as date)                                    as encounter_date,
    datediff('minute', "START", "STOP")                      as encounter_duration_minutes,
    patient                                                  as patient_id,
    organization                                             as organization_id,
    provider                                                 as provider_id,
    payer                                                    as payer_id,
    encounterclass                                           as encounter_class,
    code                                                     as encounter_code,
    description                                              as encounter_description,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    coalesce(total_claim_cost, 0) - coalesce(payer_coverage, 0) as patient_responsibility,
    case
        when total_claim_cost > 0 then payer_coverage / total_claim_cost
        else 0
    end                                                      as payer_coverage_ratio,
    reasoncode                                               as reason_code,
    reasondescription                                        as reason_description
from src
