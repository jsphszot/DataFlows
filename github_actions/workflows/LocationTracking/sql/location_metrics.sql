with

-- get completed/reviewed ipv visits
visit_ids as (
    select
    id as visit_id
    ,pal_id
    ,scheduled_for
    ,associate_type
    ,commute_started_at
    ,started_at
    ,completed_at
    ,observed_duration_mins
    -- ,commute_distance
    -- ,visit_distance
    from public.visits
    where 1=1
    and scheduled_for::date = now()::date - {dateme}
    and state in ('completed', 'reviewed')
    and is_virtual = False
    -- and successful = True
)

-- get visit events for ids in visit_ids, split into commute, start and complete
,latest_events as (
    select
    vi.visit_id
    ,ve.action
    ,ve.inserted_at
    ,row_number() over (partition by ve.visit_id, ve.action order by ve.inserted_at desc) as action_rank 
    ,case when ac.permissions::json->>'admin' = 'true' then True else False end as is_admin
    from visit_ids vi
    left join public.visit_events ve on ve.visit_id = vi.visit_id
    left join public.accounts ac on ve.account_id = ac.id
)
,commute_started_ve as (
    select * from latest_events
    where 1=1 
    and action_rank = 1
    and action = 'commute_started'
)
,visit_started_ve as (
    select * from latest_events
    where 1=1 
    and action_rank = 1
    and action = 'started'
)
,visit_completed_ve as (
    select * from latest_events
    where 1=1 
    and action_rank = 1
    and action = 'completed'
)
,ve_click_times as (
    select
    vi.*
    ,cs.inserted_at as ve_commute_started_at
    ,cs.is_admin as ve_commute_started_intervention
    ,vs.inserted_at as ve_visit_started_at
    ,vs.is_admin as ve_visit_started_intervention
    ,vc.inserted_at as ve_visit_completed_at
    ,vc.is_admin as ve_visit_completed_intervention
    ,case when cs.is_admin or vs.is_admin or vc.is_admin then True else False end as intervention_ve
    
    -- https://www.sqlines.com/postgresql/how-to/datediff
    -- ,(DATE_PART('day', vs.inserted_at - cs.inserted_at) * 24 
    --     + DATE_PART('hour', vs.inserted_at - cs.inserted_at)) * 60 
    --     + DATE_PART('minute', vs.inserted_at - cs.inserted_at
    -- ) as ve_commute_minutes
    -- ,(DATE_PART('day', vc.inserted_at - vs.inserted_at) * 24 
    --     + DATE_PART('hour', vc.inserted_at - vs.inserted_at)) * 60 
    --     + DATE_PART('minute', vc.inserted_at - vs.inserted_at
    -- ) as ve_visit_minutes
    ,round(EXTRACT(EPOCH FROM (vs.inserted_at - cs.inserted_at))::decimal/60, 1) as ve_commute_minutes
    ,round(EXTRACT(EPOCH FROM (vc.inserted_at - vs.inserted_at))::decimal/60, 1) as ve_visit_minutes
    from visit_ids vi
    left join commute_started_ve cs on vi.visit_id = cs.visit_id
    left join visit_started_ve vs on vi.visit_id = vs.visit_id
    left join visit_completed_ve vc on vi.visit_id = vc.visit_id
)


-- visit_paths -> get in-visit miles from here
,in_visit_miles_pre as (
    select
    vi.visit_id
    ,vp.visit_distance/1609 as in_visit_miles_app_calc
    ,row_number() over (partition by vi.visit_id order by vp.inserted_at desc) as ranked_rows
    from visit_ids vi
    left join public.visit_paths vp on vi.visit_id = vp.visit_id
)
,in_visit_miles as (
    select * from in_visit_miles_pre where ranked_rows = 1
)
-- visit_map_distances -> get commute miles from here
,commute_miles_pre as (
    select
    vi.visit_id
    ,vm.commute_miles_one_way as commute_miles_app_calc
    ,vm.from_location_type
    ,row_number() over (partition by vi.visit_id order by vm.inserted_at desc) as ranked_rows
    from visit_ids vi
    left join public.visit_map_distances vm on vi.visit_id = vm.visit_id
)
,commute_miles as (
    select * from commute_miles_pre where ranked_rows = 1
)

,transpo_response as (
    select
    vi.visit_id
    ,case 
        when vr.additional_tasks_completed like '%Did you provide transportation or run an errand for the member during this visit: NO%' then FALSE
        when vr.additional_tasks_completed like '%Did you provide transportation or run an errand for the member during this visit: Y%' then TRUE
        else NULL end
    as transpo_response
    from visit_ids vi
    left join public.visit_ratings vr on vi.visit_id = vr.visit_id
)


-- get visit_path_nodes data
,vpn_pre as (
    select 
    vi.visit_id
    ,vpn.inserted_at as vpn_inserted_at
    ,vpn.account_id as vpn_account_id
    ,vpn.point as vpn_point
    ,vpn.type as vpn_type
    ,vpn.odometer as vpn_odometer
    ,vpn.accuracy as vpn_accuracy
    from visit_ids vi
    left join public.visit_path_nodes vpn on vi.visit_id = vpn.visit_id
)
,vpn_node_types as (
    select 
    visit_id
    ,vpn_type
    ,count(*) as nodes_of_type
    ,max(vpn_inserted_at) as lastnodeoftype_inserted_at 
    ,min(vpn_inserted_at) as firstnodeoftype_inserted_at
    ,max(vpn_odometer) as lastnodeoftype_odometer 
    ,min(vpn_odometer) as firstnodeoftype_odometer
    from vpn_pre
    where 1=1
    and vpn_type is not null
    group by 1,2
    -- having count(*) > 1
)
,commute_started_node as (select * from vpn_node_types where vpn_type = 'commute')
,visit_started_node as (select * from vpn_node_types where vpn_type = 'start')
,visit_completed_node as (select * from vpn_node_types where vpn_type = 'complete')


,vpn_processing as (
    select 
    v.visit_id
    ,v.vpn_account_id
    
    ,v.vpn_inserted_at
    ,lag(v.vpn_inserted_at) over (partition by v.visit_id order by v.vpn_inserted_at) as lagged_vpn_inserted_at
    
    ,v.vpn_point
    ,lag(v.vpn_point) over (partition by v.visit_id order by v.vpn_inserted_at) as lagged_vpn_point
    
    ,st_x(v.vpn_point) as x
    ,st_y(v.vpn_point) as y

    ,st_x(lag(v.vpn_point) over (partition by v.visit_id order by v.vpn_inserted_at)) as lagged_x
    ,st_y(lag(v.vpn_point) over (partition by v.visit_id order by v.vpn_inserted_at)) as lagged_y

    ,v.vpn_type
    -- classify node according to visit_events Event click-times
    ,case 
        when v.vpn_inserted_at < ct.ve_commute_started_at then 'pre_commute'
        when v.vpn_inserted_at < ct.ve_visit_started_at then 'commute'
        when v.vpn_inserted_at < ct.ve_visit_completed_at then 'visit'
        else 'post_visit'
    end as ve_node_classification
    -- classify according to visit_path_nodes Type
    ,case
        -- when v.vpn_inserted_at < csn.firstnodeoftype_inserted_at then 'pre_commute'
        when v.vpn_inserted_at < csn.lastnodeoftype_inserted_at then 'pre_commute'
        -- when v.vpn_inserted_at < vsn.firstnodeoftype_inserted_at then 'commute'
        when v.vpn_inserted_at < vsn.lastnodeoftype_inserted_at then 'commute'
        -- when v.vpn_inserted_at <= vcn.firstnodeoftype_inserted_at then 'visit'
        when v.vpn_inserted_at <= vcn.lastnodeoftype_inserted_at then 'visit'
        else 'post_visit'
    end as vpntype_node_classification

    ,ct.ve_commute_started_at
    ,ct.ve_visit_started_at
    ,ct.ve_visit_completed_at
    ,ct.ve_commute_minutes
    ,ct.ve_visit_minutes
    
    ,csn.firstnodeoftype_inserted_at as vpntype_commute_started_at_fnt
    ,csn.lastnodeoftype_inserted_at as vpntype_commute_started_at_lnt

    ,vsn.firstnodeoftype_inserted_at as vpntype_visit_started_at_fnt
    ,vsn.lastnodeoftype_inserted_at as vpntype_visit_started_at_lnt

    ,vcn.firstnodeoftype_inserted_at as vpntype_visit_completed_at_fnt
    ,vcn.lastnodeoftype_inserted_at as vpntype_visit_completed_at_lnt

    -- ,round(EXTRACT(EPOCH FROM (vs.inserted_at - cs.inserted_at))::decimal/60, 1) as ve_commute_minutes
    ,round(EXTRACT(EPOCH FROM (vcn.firstnodeoftype_inserted_at - vsn.firstnodeoftype_inserted_at))::decimal/60, 1) as vpntype_visit_minutes_fnt

    ,round(EXTRACT(EPOCH FROM (vcn.lastnodeoftype_inserted_at - vsn.lastnodeoftype_inserted_at))::decimal/60, 1) as vpntype_visit_minutes_lnt
    ,round(EXTRACT(EPOCH FROM (vsn.lastnodeoftype_inserted_at - csn.lastnodeoftype_inserted_at))::decimal/60, 1) as vpntype_commute_minutes_lnt

    ,row_number() over (partition by v.visit_id order by v.vpn_inserted_at asc) as first_node
    ,row_number() over (partition by v.visit_id order by v.vpn_inserted_at desc) as last_node

    -- not used
    ,EXTRACT(EPOCH FROM (v.vpn_inserted_at - ct.ve_visit_started_at)) as seconds_between_node_ts_and_ve_started_at
    ,EXTRACT(EPOCH FROM (v.vpn_inserted_at - vsn.firstnodeoftype_inserted_at)) as seconds_between_node_ts_and_vpntype_visit_started_at_fnt

    -- used
    ,EXTRACT(EPOCH FROM (v.vpn_inserted_at - vsn.lastnodeoftype_inserted_at)) as seconds_between_node_ts_and_vpntype_visit_started_at_lnt
    ,EXTRACT(EPOCH FROM (v.vpn_inserted_at - csn.lastnodeoftype_inserted_at)) as seconds_between_node_ts_and_vpntype_commute_started_at_lnt

    from vpn_pre v
    left join ve_click_times ct on v.visit_id = ct.visit_id
    left join commute_started_node csn on csn.visit_id = v.visit_id
    left join visit_started_node vsn on vsn.visit_id = v.visit_id
    left join visit_completed_node vcn on vcn.visit_id = v.visit_id

    order by v.visit_id, v.vpn_inserted_at
)

,vpn_calcs as (
    select
    visit_id
    ,vpn_account_id
    ,ve_node_classification
    ,vpntype_node_classification

    ,EXTRACT(EPOCH FROM (vpn_inserted_at - lagged_vpn_inserted_at)) as seconds_between_nodes
    ,round((st_distance(lagged_vpn_point::geography, vpn_point::geography))::numeric, 3) meters_between_nodes
    ,(degrees(st_angle(st_point(x,lagged_y), st_point(x,y), st_point(lagged_x, lagged_y)))) as angle_between_nodes

    ,vpn_inserted_at
    ,lagged_vpn_inserted_at
    ,x
    ,y
    ,lagged_x
    ,lagged_y
    ,vpn_point
    ,lagged_vpn_point

    ,ve_commute_started_at
    ,ve_visit_started_at
    ,ve_visit_completed_at
    ,ve_commute_minutes
    ,ve_visit_minutes

    ,vpntype_commute_started_at_fnt
    ,vpntype_commute_started_at_lnt
    ,vpntype_visit_started_at_fnt
    ,vpntype_visit_started_at_lnt
    ,vpntype_visit_completed_at_fnt
    ,vpntype_visit_completed_at_lnt
    ,vpntype_visit_minutes_fnt
    ,vpntype_visit_minutes_lnt as vpntype_visit_minutes
    ,vpntype_commute_minutes_lnt as vpntype_commute_minutes


    ,seconds_between_node_ts_and_ve_started_at
    ,seconds_between_node_ts_and_vpntype_visit_started_at_fnt
    ,seconds_between_node_ts_and_vpntype_visit_started_at_lnt as seconds_between_node_ts_and_vpntype_visit_started_at
    ,seconds_between_node_ts_and_vpntype_commute_started_at_lnt as seconds_between_node_ts_and_vpntype_commute_started_at

    from vpn_processing
)

/* 
VPNTYPE Buckets
use vpn_calcs to get 5min buckets
*/

-- Commute Coverage
,vpntype_buckets_pre_commute as (
    select 
    visit_id
    ,vpn_inserted_at
    ,round(seconds_between_node_ts_and_vpntype_commute_started_at::decimal/60, 1) as minutes_between_node_ts_and_vpntype_commute_started_at
    ,vpntype_commute_minutes
    ,case when vpntype_commute_minutes = 0 then 1 else ceil(vpntype_commute_minutes/5) end as buckets -- defined as 5min buckets here
    from vpn_calcs 
    where 1=1
    and vpntype_node_classification = 'commute'
)
,vpntype_buckets_commute as (
    select 
    *
    ,case 
        when minutes_between_node_ts_and_vpntype_commute_started_at = 0 then 1
        when ceil(coalesce(minutes_between_node_ts_and_vpntype_commute_started_at/nullif(vpntype_commute_minutes, 0), 0)*buckets) > buckets then buckets
        else ceil(coalesce(minutes_between_node_ts_and_vpntype_commute_started_at/nullif(vpntype_commute_minutes, 0), 0)*buckets)
    end as which_bucket
    from vpntype_buckets_pre_commute
)
-- get how many nodes per 5minute 'bucket'
,grpd_vpntype_buckets_commute as (
    select
    visit_id,
    which_bucket,
    buckets,
    count(*) as nodes_count
    from vpntype_buckets_commute
    group by 1,2,3
)
-- get how many buckets have at least one node
,filled_vpntype_buckets_commute as (
    select 
    visit_id,
    count(*) as filled_buckets
    from grpd_vpntype_buckets_commute
    group by 1
)
,visit_vpntype_buckets_long_commute as (
    select
    gb.visit_id,
    gb.which_bucket,
    gb.nodes_count,
    gb.buckets,
    fb.filled_buckets
    from grpd_vpntype_buckets_commute gb
    left join filled_vpntype_buckets_commute fb on fb.visit_id = gb.visit_id
)
-- group bucket data to one row per visit_id
,visit_vpntype_buckets_wide_commute as (
    select
    visit_id
    ,filled_buckets
    ,buckets
    ,sum(nodes_count) as total_points
    ,coalesce(filled_buckets/buckets, 0) as coverage
    ,string_agg(which_bucket || ': ' || nodes_count, ', ' order by which_bucket asc) as buckets_and_points
    from visit_vpntype_buckets_long_commute
    group by 1,2,3
)


-- Visit Coverage
,vpntype_buckets_pre as (
    select 
    visit_id
    ,vpn_inserted_at
    ,round(seconds_between_node_ts_and_vpntype_visit_started_at::decimal/60, 1) as minutes_between_node_ts_and_vpntype_visit_started_at
    ,vpntype_visit_minutes
    ,case when vpntype_visit_minutes = 0 then 1 else ceil(vpntype_visit_minutes/5) end as buckets -- defined as 5min buckets here
    from vpn_calcs 
    where 1=1
    and vpntype_node_classification = 'visit'
)
,vpntype_buckets as (
    select 
    *
    ,case 
        when minutes_between_node_ts_and_vpntype_visit_started_at = 0 then 1
        when ceil(coalesce(minutes_between_node_ts_and_vpntype_visit_started_at/nullif(vpntype_visit_minutes, 0), 0)*buckets) > buckets then buckets
        else ceil(coalesce(minutes_between_node_ts_and_vpntype_visit_started_at/nullif(vpntype_visit_minutes, 0), 0)*buckets)
    end as which_bucket
    from vpntype_buckets_pre
)
-- get how many nodes per 5minute 'bucket'
,grpd_vpntype_buckets as (
    select
    visit_id,
    which_bucket,
    buckets,
    count(*) as nodes_count
    from vpntype_buckets
    group by 1,2,3
)
-- get how many buckets have at least one node
,filled_vpntype_buckets as (
    select 
    visit_id,
    count(*) as filled_buckets
    from grpd_vpntype_buckets
    group by 1
)
-- join grpd_bucket data with filled_buckets data
,visit_vpntype_buckets_long as (
    select
    gb.visit_id,
    gb.which_bucket,
    gb.nodes_count,
    gb.buckets,
    fb.filled_buckets
    from grpd_vpntype_buckets gb
    left join filled_vpntype_buckets fb on fb.visit_id = gb.visit_id
)
-- group bucket data to one row per visit_id
,visit_vpntype_buckets_wide as (
    select
    visit_id
    ,filled_buckets
    ,buckets
    ,sum(nodes_count) as total_points
    ,coalesce(filled_buckets/buckets, 0) as coverage
    ,string_agg(which_bucket || ': ' || nodes_count, ', ' order by which_bucket asc) as buckets_and_points
    from visit_vpntype_buckets_long
    group by 1,2,3
)

/* 
As The Crow Flies
use vpn_calcs to other metrics
[] % of visits with a `commute`, `start`, and `complete` visit_path_node - this may need to be broken into each point type
[x] % of visit_path_nodes that fall within a visits `commute_started_at` -> `completed_at` time stamps.
[x]  total visit_path_nodes vs total visit distance - this would help us understand roughly how many nodes per mile traveled are needed to gain a "good" visit path.
[] average time between nodes for a visit, as well as outliers - again this would help us understand breaking points on our data that may help guide features/improvements in the future for us to further refine location tracking.
[] nodes coming in after `completed_at_time`.
*/

,node_metrics as (
    select
    visit_id
    ,avg(seconds_between_nodes) as avg_seconds_between_nodes
    ,avg(meters_between_nodes) as avg_meters_between_nodes    

    ,max(seconds_between_nodes) as max_seconds_between_nodes
    ,max(meters_between_nodes) as max_meters_between_nodes
    
    ,sum(meters_between_nodes) as total_nodes_distance_meters
    ,round(sum(meters_between_nodes)/1609, 1) as total_nodes_distance_miles

    ,count(*) as nodes_count
    ,percentile_cont(0.50) within group (order by seconds_between_nodes) as sec_between_nodes_50pile
    ,percentile_cont(0.25) within group (order by seconds_between_nodes) as sec_between_nodes_25pile
    ,percentile_cont(0.75) within group (order by seconds_between_nodes) as sec_between_nodes_75pile
    ,percentile_cont(0.10) within group (order by seconds_between_nodes) as sec_between_nodes_10pile
    ,percentile_cont(0.90) within group (order by seconds_between_nodes) as sec_between_nodes_90pile

    from vpn_calcs
    group by 1
)

,node_metrics_vpntype_nodeclass as (
    select
    visit_id, vpntype_node_classification
    ,avg(seconds_between_nodes) as avg_seconds_between_nodes
    ,avg(meters_between_nodes) as avg_meters_between_nodes    

    ,max(seconds_between_nodes) as max_seconds_between_nodes
    ,max(meters_between_nodes) as max_meters_between_nodes
    
    ,sum(meters_between_nodes) as total_nodes_distance_meters
    ,round(sum(meters_between_nodes)/1609, 1) as total_nodes_distance_miles

    ,count(*) as nodes_count

    ,percentile_cont(0.50) within group (order by seconds_between_nodes) as sec_between_nodes_50pile
    ,percentile_cont(0.25) within group (order by seconds_between_nodes) as sec_between_nodes_25pile
    ,percentile_cont(0.75) within group (order by seconds_between_nodes) as sec_between_nodes_75pile
    ,percentile_cont(0.10) within group (order by seconds_between_nodes) as sec_between_nodes_10pile
    ,percentile_cont(0.90) within group (order by seconds_between_nodes) as sec_between_nodes_90pile

    from vpn_calcs
    -- where vpntype_node_classification = 'visit'
    group by 1, 2
)
,node_metrics_pre_commute as (select * from node_metrics_vpntype_nodeclass where vpntype_node_classification = 'pre_commute')
,node_metrics_commute as (select * from node_metrics_vpntype_nodeclass where vpntype_node_classification = 'commute')
,node_metrics_visit as (select * from node_metrics_vpntype_nodeclass where vpntype_node_classification = 'visit')
,node_metrics_post_visit as (select * from node_metrics_vpntype_nodeclass where vpntype_node_classification = 'post_visit')


,final as (
    select
    vi.visit_id

    -- ,nmc.nodes_count as commute_nodes
    -- ,nmv.nodes_count as visit_nodes
    -- ,nm.nodes_count as total_nodes
    -- ,((nmc.nodes_count+nmv.nodes_count)/nullif(nm.nodes_count,0)) as percent_nodes_in_commute_started_to_visit_completed


    ,vi.pal_id
    ,vi.scheduled_for
    ,vi.associate_type
    ,tr.transpo_response
    ,vi.commute_started_at as v_commute_started_at
    ,csn.lastnodeoftype_inserted_at as commute_node_type_inserted_at
    ,vi.started_at as v_visit_started_at
    ,vsn.lastnodeoftype_inserted_at as start_node_type_inserted_at
    ,vi.completed_at as v_visit_completed_at
    ,vcn.lastnodeoftype_inserted_at as complete_node_inserted_at
    ,vi.observed_duration_mins as v_observed_duration_mins
    ,cm.commute_miles_app_calc
    ,(vsn.lastnodeoftype_odometer-csn.lastnodeoftype_odometer)/1609 as commute_miles_odometer
    ,ivm.in_visit_miles_app_calc
    ,(vcn.lastnodeoftype_odometer-vsn.lastnodeoftype_odometer)/1609 as in_visit_miles_odometer

    ,csn.nodes_of_type as commute_node_type_count
    ,vsn.nodes_of_type as start_node_type_count
    ,vcn.nodes_of_type as complete_node_type_count

    ,csn.lastnodeoftype_odometer as commute_node_type_odometer
    ,vsn.lastnodeoftype_odometer as start_node_type_odometer
    ,vcn.lastnodeoftype_odometer as complete_node_type_odometer
    

    ,bkts.filled_buckets as in_visit_filled_buckets
    ,bkts.buckets as in_visit_buckets
    ,bkts.buckets_and_points as in_visit_buckets_and_nodes_list
    ,bkts.coverage as in_visit_location_coverage

    ,bkts_c.filled_buckets as commute_filled_buckets
    ,bkts_c.buckets as commute_buckets
    ,bkts_c.buckets_and_points as commute_buckets_and_nodes_list
    ,bkts_c.coverage as commute_location_coverage

    -- percent of nodes that fall within commute_start to completed_at
    -- total visit_path_nodes vs total_distance
    -- ,((nmc.nodes_count+nmv.nodes_count)/nullif(nm.nodes_count,0)) as percent_nodes_in_commute_started_to_visit_completed
    ,((coalesce(nmc.nodes_count, 0)::decimal+coalesce(nmv.nodes_count, 0)::decimal)/coalesce(nm.nodes_count,0)::decimal) as percent_nodes_in_commute_started_to_visit_completed
    ,nmv.nodes_count/nullif(nmv.total_nodes_distance_miles,0) as visit_nodes_count_over_sum_miles_between_visit_nodes
    ,nmv.nodes_count/nullif(ivm.in_visit_miles_app_calc,0) as visit_nodes_count_over_visit_path_table_miles

    ,nm.avg_seconds_between_nodes
    ,nm.avg_meters_between_nodes
    ,nm.max_seconds_between_nodes
    ,nm.max_meters_between_nodes
    ,nm.total_nodes_distance_meters
    ,nm.total_nodes_distance_miles
    ,nm.nodes_count
    ,nm.sec_between_nodes_50pile
    ,nm.sec_between_nodes_25pile
    ,nm.sec_between_nodes_75pile
    ,nm.sec_between_nodes_10pile
    ,nm.sec_between_nodes_90pile

    ,nmpc.avg_seconds_between_nodes as avg_seconds_between_nodes_precommute
    ,nmpc.avg_meters_between_nodes as avg_meters_between_nodes_precommute
    ,nmpc.max_seconds_between_nodes as max_seconds_between_nodes_precommute
    ,nmpc.max_meters_between_nodes as max_meters_between_nodes_precommute
    ,nmpc.total_nodes_distance_meters as total_nodes_distance_meters_precommute
    ,nmpc.total_nodes_distance_miles as total_nodes_distance_miles_precommute
    ,nmpc.nodes_count as nodes_count_precommute
    ,nmpc.sec_between_nodes_50pile as sec_between_nodes_50pile_precommute
    ,nmpc.sec_between_nodes_25pile as sec_between_nodes_25pile_precommute
    ,nmpc.sec_between_nodes_75pile as sec_between_nodes_75pile_precommute
    ,nmpc.sec_between_nodes_10pile as sec_between_nodes_10pile_precommute
    ,nmpc.sec_between_nodes_90pile as sec_between_nodes_90pile_precommute

    ,nmc.avg_seconds_between_nodes as avg_seconds_between_nodes_commute
    ,nmc.avg_meters_between_nodes as avg_meters_between_nodes_commute
    ,nmc.max_seconds_between_nodes as max_seconds_between_nodes_commute
    ,nmc.max_meters_between_nodes as max_meters_between_nodes_commute
    ,nmc.total_nodes_distance_meters as total_nodes_distance_meters_commute
    ,nmc.total_nodes_distance_miles as total_nodes_distance_miles_commute
    ,nmc.nodes_count as nodes_count_commute
    ,nmc.sec_between_nodes_50pile as sec_between_nodes_50pile_commute
    ,nmc.sec_between_nodes_25pile as sec_between_nodes_25pile_commute
    ,nmc.sec_between_nodes_75pile as sec_between_nodes_75pile_commute
    ,nmc.sec_between_nodes_10pile as sec_between_nodes_10pile_commute
    ,nmc.sec_between_nodes_90pile as sec_between_nodes_90pile_commute

    ,nmv.avg_seconds_between_nodes as avg_seconds_between_nodes_visit
    ,nmv.avg_meters_between_nodes as avg_meters_between_nodes_visit
    ,nmv.max_seconds_between_nodes as max_seconds_between_nodes_visit
    ,nmv.max_meters_between_nodes as max_meters_between_nodes_visit
    ,nmv.total_nodes_distance_meters as total_nodes_distance_meters_visit
    ,nmv.total_nodes_distance_miles as total_nodes_distance_miles_visit
    ,nmv.nodes_count as nodes_count_visit
    ,nmv.sec_between_nodes_50pile as sec_between_nodes_50pile_visit
    ,nmv.sec_between_nodes_25pile as sec_between_nodes_25pile_visit
    ,nmv.sec_between_nodes_75pile as sec_between_nodes_75pile_visit
    ,nmv.sec_between_nodes_10pile as sec_between_nodes_10pile_visit
    ,nmv.sec_between_nodes_90pile as sec_between_nodes_90pile_visit

    ,nmpv.avg_seconds_between_nodes as avg_seconds_between_nodes_post_visit
    ,nmpv.avg_meters_between_nodes as avg_meters_between_nodes_post_visit
    ,nmpv.max_seconds_between_nodes as max_seconds_between_nodes_post_visit
    ,nmpv.max_meters_between_nodes as max_meters_between_nodes_post_visit
    ,nmpv.total_nodes_distance_meters as total_nodes_distance_meters_post_visit
    ,nmpv.total_nodes_distance_miles as total_nodes_distance_miles_post_visit
    ,nmpv.nodes_count as nodes_count_post_visit
    ,nmpv.sec_between_nodes_50pile as sec_between_nodes_50pile_post_visit
    ,nmpv.sec_between_nodes_25pile as sec_between_nodes_25pile_post_visit
    ,nmpv.sec_between_nodes_75pile as sec_between_nodes_75pile_post_visit
    ,nmpv.sec_between_nodes_10pile as sec_between_nodes_10pile_post_visit
    ,nmpv.sec_between_nodes_90pile as sec_between_nodes_90pile_post_visit


    from visit_ids vi
    left join in_visit_miles ivm on ivm.visit_id = vi.visit_id
    left join commute_miles cm on cm.visit_id = vi.visit_id
    left join transpo_response tr on tr.visit_id = vi.visit_id
    left join commute_started_node csn on csn.visit_id = vi.visit_id
    left join visit_started_node vsn on vsn.visit_id = vi.visit_id
    left join visit_completed_node vcn on vcn.visit_id = vi.visit_id
    left join visit_vpntype_buckets_wide bkts on bkts.visit_id = vi.visit_id
    left join visit_vpntype_buckets_wide_commute bkts_c on bkts_c.visit_id = vi.visit_id
    left join node_metrics nm on nm.visit_id = vi.visit_id
    left join node_metrics_pre_commute nmpc on nmpc.visit_id = vi.visit_id
    left join node_metrics_commute nmc on nmc.visit_id = vi.visit_id
    left join node_metrics_visit nmv on nmv.visit_id = vi.visit_id
    left join node_metrics_post_visit nmpv on nmpv.visit_id = vi.visit_id
)

select * from final
where 1=1
