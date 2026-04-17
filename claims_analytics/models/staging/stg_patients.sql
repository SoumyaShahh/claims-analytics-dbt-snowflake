{{ config(materialized='view') }}

with source as (
    select * from {{ source('synthea_raw', 'raw_patients') }}
),

renamed as (
    select
        id                                                           as patient_id,
        birthdate                                                    as birth_date,
        deathdate                                                    as death_date,
        case when deathdate is null then true else false end          as is_alive,
        datediff('year', birthdate, coalesce(deathdate, current_date())) as age_years,
        prefix                                                        as name_prefix,
        first                                                         as first_name,
        last                                                          as last_name,
        suffix                                                        as name_suffix,
        marital                                                       as marital_status,
        race,
        ethnicity,
        gender,
        birthplace,
        city,
        state,
        county,
        zip                                                           as zip_code,
        lat                                                           as latitude,
        lon                                                           as longitude,
        healthcare_expenses                                           as lifetime_healthcare_expenses,
        healthcare_coverage                                           as lifetime_healthcare_coverage
    from source
)

select * from renamed