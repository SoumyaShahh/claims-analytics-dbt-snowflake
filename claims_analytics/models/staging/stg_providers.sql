{{ config(materialized='view') }}

with src as (
    select * from {{ source('synthea_raw', 'raw_providers') }}
)

select
    id                          as provider_id,
    organization                as organization_id,
    name                        as provider_name,
    gender                      as provider_gender,
    speciality                  as specialty,
    address,
    city,
    state,
    zip                         as zip_code,
    lat                         as latitude,
    lon                         as longitude,
    utilization                 as encounter_count
from src
