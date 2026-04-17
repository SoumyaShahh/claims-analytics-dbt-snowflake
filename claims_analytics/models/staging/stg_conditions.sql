{{ config(materialized='view') }}

with source as (
    select * from {{ source('synthea_raw', 'raw_conditions') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['patient', 'encounter', 'code', '"START"']) }} as condition_id,
        "START"                                                  as condition_start_date,
        "STOP"                                                   as condition_end_date,
        case when "STOP" is null then true else false end        as is_active,
        case
            when "STOP" is null then null
            else datediff('day', "START", "STOP")
        end                                                      as condition_duration_days,
        patient                                                  as patient_id,
        encounter                                                as encounter_id,
        code                                                     as condition_code,
        description                                              as condition_description
    from source
)

select * from renamed