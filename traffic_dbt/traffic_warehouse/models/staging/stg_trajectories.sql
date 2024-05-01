with source as (
    select *
    from {{ ref('trajectorydf') }}
),

stage_trajectories as (
    select
        track_id,
        type,
        lat,
        lon,
        speed,
        lon_acc,
        lat_acc,
        time 
    from source
)

select *
from stage_trajectories