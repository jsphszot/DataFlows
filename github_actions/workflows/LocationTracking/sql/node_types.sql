-- quick count of "drive-time", "walk-time" etc for visits

with

-- get completed/reviewed ipv visits
visit_ids as (
    select
    id as visit_id
    ,pal_id
    ,papa_id
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
    -- and pal_id='b6b9cad5-b28c-473e-b63c-6b8267da80e2'
    -- and successful = True
    -- limit 10
)
-- get visit_path_nodes data
,vpn_pre as (
    select 
    vpn.*
    -- ,vi.pal_id
    ,p.id as pal_id
    ,vi.papa_id
        -- ,id
        -- ,visit_id
        -- ,account_id
        -- ,inserted_at
        -- ,updated_at
        -- ,point
        -- ,type
        -- ,odometer
        -- ,accuracy
        -- ,is_moving
        -- ,speed
        -- ,speed_accuracy
        -- ,activity_type
        -- ,activity_confidence

    from public.visit_path_nodes vpn
    left join public.pals p on p.account_id=vpn.account_id
    -- where vpn.visit_id in (select distinct visit_id from visit_ids)
    inner join visit_ids vi on vi.visit_id = vpn.visit_id
    -- left join public.visit_path_nodes vpn on vi.visit_id = vpn.visit_id
)
,vpn_node_types as (
    select 
    visit_id
    ,pal_id
    ,type as node_type
    ,count(*) as nodes_of_type
    ,max(inserted_at) as lastnodeoftype_inserted_at 
    ,min(inserted_at) as firstnodeoftype_inserted_at
    ,max(odometer) as lastnodeoftype_odometer 
    ,min(odometer) as firstnodeoftype_odometer
    from vpn_pre
    where 1=1
    and type is not null
    group by 1,2,3
    -- having count(*) > 1
)
,commute_started_node as (select * from vpn_node_types where node_type = 'commute')
,visit_started_node as (select * from vpn_node_types where node_type = 'start')
,visit_completed_node as (select * from vpn_node_types where node_type = 'complete')

,vpn_processing as (
    select 
    v.*
    
    ,EXTRACT(EPOCH FROM (v.inserted_at - lag(v.inserted_at) over (partition by v.visit_id order by v.inserted_at))) as seconds_between_nodes
    ,round((st_distance(lag(v.point) over (partition by v.visit_id order by v.inserted_at)::geography, v.point::geography))::numeric, 3) meters_between_nodes

    ,csn.lastnodeoftype_inserted_at pre_commute_node_at
    ,vsn.lastnodeoftype_inserted_at commute_node_at
    ,vcn.lastnodeoftype_inserted_at visit_node_at
    -- classify according to visit_path_nodes Type
    ,case
        when v.inserted_at < csn.lastnodeoftype_inserted_at then 'pre_commute'
        when v.inserted_at < vsn.lastnodeoftype_inserted_at then 'commute'
        when v.inserted_at <= vcn.lastnodeoftype_inserted_at then 'visit'
        else 'post_visit'
    end as part_of_visit

    from vpn_pre v
    left join commute_started_node csn on csn.visit_id = v.visit_id
    left join visit_started_node vsn on vsn.visit_id = v.visit_id
    left join visit_completed_node vcn on vcn.visit_id = v.visit_id

    order by v.visit_id, v.inserted_at
)
,grouped_vpn_by_visit as (
    select 
    -- * 
    visit_id
    ,pal_id
    ,papa_id
    ,activity_type
    ,part_of_visit
    ,count(*) as count
    ,sum(seconds_between_nodes) seconds
    ,sum(meters_between_nodes) meters
    ,min(inserted_at) as first_node_inserted_at
    ,max(inserted_at) as last_node_inserted_at
    from vpn_processing
    group by 1,2,3,4,5
)

-- does pal have driving permission? pal_onboarding seems incomplete, there is other data in pals.fountain
,pal_can_drive as (
    select
    p.account_id
    ,p.id as pal_id
    ,p.vehicle
    ,p.fountain_data::json->'applicant'->'data'->>'drivers_license_number' as dl_number_fountain
    -- ,a.no_driving account_no_driving -- seems useless (might be a ban from driving ?)
    ,po.vehicle_insurance_approved_by_id as vi_approved_by_id_pal_onboarding
    ,case when po.vehicle_insurance_approved_by_id is not null then true else false end as vi_approved_pal_onboarding
    from public.pals p 
    left join public.pal_onboarding po on p.account_id=po.account_id
    left join public.accounts a on a.id=p.account_id
)

-- do visit tasks require vehicle ?
,ungrouped_visit_task as (
    select 
    -- vt.*
    vt.visit_id
    ,t.name
    ,t.description
    ,t.requires_vehicle
    ,t.tracks_mileage
    from public.visit_tasks vt
    left join public.tasks t on t.id=vt.task_id
    -- limit 100
)
,visit_tasks_req_vehicle as (
  select
  visit_id
  ,sum(case when requires_vehicle=true then 1 else 0 end) as tasks_requiring_vehicle
  from ungrouped_visit_task
  group by 1
)

-- does the papa's business allow driving ?
,papas_and_business_allowed_tasks as (
  select
  p.id as papa_id
  ,p.status as papa_status
  ,b.name as business_name
  -- ,b.id as business_id
  ,t.name as task_name
  ,t.legacy_services as task_legacy_services
  ,t.requires_vehicle as task_requires_vehicle
  from public.papas p
  left join public.businesses b on p.business_id=b.id
  left join public.business_policies bp on b.id=bp.business_id
  left join allowed_tasks at on bp.id=at.business_policy_id
  left join tasks t on t.id = at.task_id
)
,business_can_drive as (
    select
    papa_id
    ,papa_status
    ,business_name
    ,sum(case when task_requires_vehicle=true then 1 else 0 end)>0 as business_allows_driving
    from papas_and_business_allowed_tasks
    group by 1,2,3
)

,vpn_joined_accounts as (
    select 
    -- vpn.pal_id
    -- ,vpn.papa_id
    -- ,v.required_vehicle_type
    -- ,v.services
    vpn.pal_id
    ,vpn.papa_id
    ,vpn.visit_id
    ,pcd.vi_approved_pal_onboarding
    ,pcd.dl_number_fountain
    ,pcd.vehicle
    ,vt.tasks_requiring_vehicle
    ,bcd.business_allows_driving
    ,vpn.activity_type
    ,vpn.part_of_visit
    ,vpn.first_node_inserted_at
    ,vpn.last_node_inserted_at
    ,vpn.count
    ,vpn.seconds
    ,vpn.meters
    ,v.visit_distance
    ,case when vr.additional_tasks_completed like '%Did you provide transportation or run an errand for the member during this visit: Y%' then true else false end as transpo_provided_pal_response
    -- ,vpn.*
    from grouped_vpn_by_visit vpn
    left join public.visits v on v.id=vpn.visit_id
    left join pal_can_drive pcd on pcd.pal_id=vpn.pal_id
    left join visit_tasks_req_vehicle vt on vt.visit_id=vpn.visit_id
    left join business_can_drive bcd on bcd.papa_id=vpn.papa_id
    left join public.visit_ratings vr on vr.visit_id=vpn.visit_id
)

select * from vpn_joined_accounts
where 1=1 
-- and activity_type='in_vehicle'
-- and part_of_visit='visit'
