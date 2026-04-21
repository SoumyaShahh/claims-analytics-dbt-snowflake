{{ config(materialized='table') }}

with p as (
    select * from {{ ref('stg_providers') }}
),
o as (
    select * from {{ ref('stg_organizations') }}
)

select
    p.provider_id,
    p.provider_name,
    p.provider_gender,
    p.specialty,
    p.encounter_count                            as provider_encounter_count,
    p.city                                       as provider_city,
    p.state                                      as provider_state,
    p.zip_code                                   as provider_zip_code,
    p.organization_id,
    o.organization_name,
    o.city                                       as organization_city,
    o.state                                      as organization_state,
    o.total_revenue                              as organization_total_revenue,
    o.total_utilization                          as organization_total_utilization
from p
left join o on p.organization_id = o.organization_id
