with source as (
    select *
    from {{ ref('trackdf') }}
),

stage_vehicles as (
    select
        track_id,
        type,
        traveled_d,
        avg_speed
    from source
)

select *
from stage_vehicles 