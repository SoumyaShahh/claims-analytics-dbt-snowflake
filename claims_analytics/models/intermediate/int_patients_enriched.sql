{{ config(materialized='ephemeral') }}

with patients as (
    select * from {{ ref('stg_patients') }}
),

enriched as (
    select
        patient_id,
        first_name,
        last_name,
        birth_date,
        death_date,
        is_alive,
        age_years,
        gender,
        race,
        ethnicity,
        marital_status,
        city,
        state,
        county,
        zip_code,
        lifetime_healthcare_expenses,
        lifetime_healthcare_coverage,
        lifetime_healthcare_expenses - lifetime_healthcare_coverage as lifetime_out_of_pocket,

        case
            when age_years < 18 then 'Pediatric (0-17)'
            when age_years between 18 and 34 then 'Young Adult (18-34)'
            when age_years between 35 and 49 then 'Adult (35-49)'
            when age_years between 50 and 64 then 'Middle Age (50-64)'
            when age_years between 65 and 74 then 'Senior (65-74)'
            when age_years >= 75 then 'Elderly (75+)'
            else 'Unknown'
        end as age_band,

        case when age_years >= 65 then true else false end as is_medicare_eligible

    from patients
)

select * from enriched