-- for a Pal-Member pair, stats on:
-- - total completed visits
-- - latest visit at
-- - first completed visit as a Prefered connection at
-- - completed visit count when PP connection happens
-- - PP connection rank (if Pal-Member pair have connected/deconnected Prefered status)

with
pp_connections as (
    select * from {{ ref('papa_favorite_pals') }}
)
,pal_papa_succesful_visits as ( -- CURRENT count of completed visits between Papa and Pal 
    select
    pal_id, papa_id
    -- ,id as visit_id
    ,count(*) as completeds
    ,max(completed_at) as latest_visit_completed_at
    ,case when completeds>=3 then '≥3' else completeds::text end as completeds_grouping
    from {{ ref('visit') }}
    where 1=1
    {# and completed_at is not null and terminated_at is null and successful=true #}
    and status in ('reviewed', 'completed')
    and is_virtual=false 
    group by all
)
,papa_pal_succesful_visits_windowed as ( -- count of completed visits between Papa and Pal through time
    select
    papa_id, pal_id
    ,row_number() over (partition by papa_id, pal_id order by completed_at) completeds
    ,completed_at as valid_from
    ,lead(completed_at) over (partition by papa_id, pal_id order by completed_at) as first_completed_at_after_pp_connection
    ,coalesce(lead(completed_at) over (partition by papa_id order by completed_at), '9999-09-09') valid_to
    from {{ ref('visit') }}
    where 1=1 
    and is_virtual=false
    {# and completed_at is not null and terminated_at is null and successful=true #}
    and status in ('reviewed', 'completed')
)
,final as (
    select 
    pp.* 
    ,coalesce(sv.completeds, 0) as completed_visits
    ,coalesce(sv.completeds_grouping, '0') as completed_visits_grouping
    ,sv.latest_visit_completed_at
    
    ,ppsv.first_completed_at_after_pp_connection
    ,ppsv.completeds as visits_completed_at_pp_connect
    ,dense_rank() over (partition by pp.papa_id, pp.pal_id order by pp.valid_from asc) as connection_rank
    
    from pp_connections pp
    left join pal_papa_succesful_visits sv on (pp.pal_id||pp.papa_id)=(sv.pal_id||sv.papa_id)

    left join papa_pal_succesful_visits_windowed ppsv on (1=1 
        and pp.papa_id||pp.pal_id=ppsv.papa_id||ppsv.pal_id
        and pp.valid_from between ppsv.valid_from and ppsv.valid_to
        )
)

select * from final
