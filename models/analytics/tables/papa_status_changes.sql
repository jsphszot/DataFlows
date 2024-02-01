with
non_deduped as ( -- get all logs
    select
    papa_id
    ,inserted_at::date as inserted_date
    ,new_status
    ,old_status
    ,count(*) as cnts
    ,min(inserted_at) as min_inserted_at
    from
    {{ ref('papa_status_log') }}
    psl
    where 1=1
    group by all
)
,daily_initial_and_final_state as ( -- have only one state change per papa per day
    select
    papa_id
    , inserted_date
    , min_by(old_status, min_inserted_at) as bod_state
    , max_by(new_status, min_inserted_at) as eod_state
    , listagg(old_status || ' -> ' || new_status, ' ::: ') within group (order by min_inserted_at asc) as state_changes
    from non_deduped
    group by all
    order by inserted_date desc
)
,papa_state_cte as ( -- join to Papa for Geo home location
    select
    psl.papa_id
    , psl.inserted_date as state_changed_date
    , coalesce(lead(psl.inserted_date) over (partition by psl.papa_id order by psl.inserted_date), '9999-01-01') as new_status_valid_until_date
    , case when psl.bod_state like any ('active%') then 'active' when psl.bod_state like any ('inactive', 'ineligible') then psl.bod_state else 'other' end as bod_state
    , case when psl.eod_state like any ('active%') then 'active' when psl.eod_state like any ('inactive', 'ineligible') then psl.eod_state else 'other' end as eod_state
    , p.state as papa_home_state
    , p.county_state as papa_home_county_state
    , p.organization_name as papa_organization_name
    from daily_initial_and_final_state psl
    left join {{ ref('papa') }} p on p.id=psl.papa_id
)
,dim_dated_logs as (
    select
    d.date::date as date
    , coalesce(lag(p.eod_state) over (partition by p.papa_id order by d.date), p.bod_state) as bod_state_clean
    , p.eod_state as eod_state_clean
    , (d.date::date=p.state_changed_date)::int as has_state_change
    , case when d.date::date<>p.state_changed_date then eod_state_clean else bod_state_clean || ' -> ' || eod_state_clean end as delta_state
    ,p.*
    from papa_state_cte p
    left join {{ ref('date_sequence') }} d on d.date::date>=p.state_changed_date and d.date::date<p.new_status_valid_until_date
    where 1=1
    and d.date<=sysdate()::date
    order by d.date desc
)

select * from dim_dated_logs 
