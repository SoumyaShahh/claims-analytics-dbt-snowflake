{{ config(materialized='view') }}

with src as (
    select * from {{ source('synthea_raw', 'raw_organizations') }}
)

select
    id              as organization_id,
    name            as organization_name,
    address,
    city,
    state,
    zip             as zip_code,
    lat             as latitude,
    lon             as longitude,
    phone,
    revenue         as total_revenue,
    utilization     as total_utilization
from src
