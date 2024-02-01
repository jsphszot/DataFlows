
{%- set visits_since = "'2022-02-01'" -%}
{# dac data_after_change, dbc data_before_change  #}
{%- set data_change_vals = [
    {'src': 'dac', 'ffill': true, 'val':'pal_id', 'cast':'string', 'rename':'pal_id',},
    {'src': 'dbc', 'ffill': true, 'val':'pal_id', 'cast':'string', 'rename':'pal_id',},
    {'src': 'dac', 'ffill': true, 'val':'visit_partner_id', 'cast':'string', 'rename':'visit_partner_id',},
    {'src': 'dac', 'ffill': true, 'val':'is_virtual', 'cast':'boolean', 'rename':'is_virtual',},
    {'src': 'dac', 'ffill': true, 'val':'state', 'cast':'string', 'rename': 'visit_status'},
    {'src': 'dac', 'ffill': true, 'val':'favorite_pals_only', 'cast':'boolean', 'rename':'favorite_pals_only',},
    {'src': 'dac', 'ffill': true, 'val':'scheduled_for', 'cast':'timestamp', 'rename':'scheduled_for',},
    {'src': 'dac', 'ffill': false, 'val':'required_vehicle_type', 'cast':'string', 'rename':'required_vehicle_type',},
    {'src': 'dbc', 'ffill': false, 'val':'required_vehicle_type', 'cast':'string', 'rename':'required_vehicle_type',},
] -%}
with 
{# -- I. source tables #}
visit_events_src as (
    select * from {{ ref('visit_events') }}
)
,account_src as (
    select * from {{ ref('account') }}
)
,pal_src as (
    select * from {{ ref('pal') }}
)
{# -- II. processing #}
,visit_events as (
    select
        visit_id
        ,id as event_id
        ,account_id
        ,pal_id
        ,action
        ,'visit_event' as event_source
        ,reason_code
        ,reason_description
        ,inserted_at
        ,updated_at
        ,case when action='flagged' then null else data_after_change end as dac
        ,case when action='flagged' then null else data_before_change end as dbc
    from visit_events_src
    where 1=1
)
,communication_logs as (
    select 
    visit_id
    ,id as event_id
    ,regexp_substr("TO", '[^Account ]([a-z\\d\\-]+)')::text as account_id
    ,null as pal_id
    ,type as action
    ,'communication_logs' as event_source
    ,notification_name as reason_code
    {# ,body as reason_description #}
    ,null as reason_description
    ,inserted_at
    ,updated_at
    ,null as dac
    ,null as dbc
    from {{ ref('src_communication_logs') }}
    where 1=1
    and visit_id is not null
    and type = 'PN' {# to avoid bringing in PII (account phone number from SMS events) #}
)
,vs_comments as (
    select 
    visit_id
    ,id as event_id
    ,creator_id as account_id
    ,null as pal_id
    ,'vs_comment' as action
    ,'visit_comments' as event_source
    ,regexp_substr(regexp_substr(upper(content), '[^A-Z](VS[A-Z_\\-\\)]+)|(^VS[A-Z_\\-\\)])'), '(VS.+)') as reason_code
    ,null as reason_description
    {# ,upper(content) as reason_description -- This probably contains PII #}
    ,inserted_at
    ,updated_at
    ,null as dac
    ,null as dbc
    from {{ ref('visit_comments') }}
    where 1=1
    and visit_id is not null
    and reason_code is not null
)
,ivr_calls as (
    select
    params:visit_id::string as visit_id
    ,id as event_id
    ,null as account_id
    ,null as pal_id
    ,type as action
    ,'ivr_calls' as event_source
    ,upper(status) as reason_code
    ,params:reason::string as reason_description
    ,inserted_at
    ,updated_at
    ,null as dac
    ,null as dbc
    from {{ source('member_communications', 'IVR_CALLS') }}
)

, priority_bonus as (
    SELECT  
        visit_id,
        inserted_at,
        valid_until,
        amount_in_cents/100 as priority_bonus_at_event
    FROM {{ref('priority_bonus_event')}} a    
)

{#
segment events are NOT all similar structure, this looping logic may make no sense
as each one might need to be treated specifically before unioning / joining
#}
{# -- add new tables here #}
{%- set segment_tables = [
    {'name': 'cancel_visit_button_clicked', 'schema': 'papa_pal_prod'},
    {'name': 'yes_cancel_clicked', 'schema': 'papa_pal_prod'},
    ]
-%}
{# -- in here union visit_events with other tables (segment, cancellations, etc) #}
,visit_events_uu as (
    select * from visit_events
    union
    select * from communication_logs
    union
    select * from vs_comments
    union
    select * from ivr_calls
    {%- if segment_tables | length > 0 %}
    {%- for table in segment_tables %}
    union
    select
    visit_id
    ,id as event_id
    ,user_id as account_id
    ,context_traits_pal_id as pal_id
    {# ,'{{ table['name'] | replace('_', ' ') }}' as action #}
    ,'{{ table['name'] }}' as action
    ,'segment_events' as event_source
    ,null as reason_code
    ,null as reason_description
    ,original_timestamp as inserted_at
    ,original_timestamp as updated_at
    ,null as dac
    ,null as dbc
    from segment_events.{{ table['schema'] }}.{{ table['name'] }}    
    {%- endfor %}
    {%- endif %}
)
,visit_events_u as (
        select 
        * 
        {# extract from json  #}
        {%- for val in data_change_vals %}
        ,{{ val['src'] }}:{{ val['val'] }}::{{ val['cast'] }} as {{val['rename']}}_{{ val['src'] }}
        {%- endfor %}
        from visit_events_uu
)
{# use visit_id_list as a filter to run tests  #}
{# 
,visit_id_pre_list as (
    select distinct visit_id
    from visit_events_src
    where 1=1
    -- and action like any ('%partner%') 
    -- and inserted_at > current_timestamp - interval '240 hours'
) 
#}
,visit_id_list as (
    select distinct id as visit_id
    from {{ ref('visit') }}
    where 1=1
    and inserted_at > {{ visits_since }}
    {# and is_virtual = FALSE #}
    {# and visit_id in (select * from visit_id_pre_list) #}
)
{# -- data from visit events #}
,visit_events_actor_data as (
    select 
    ve.visit_id
    ,ve.event_id
    ,ve.action
    {%- for val in data_change_vals %}
    ,ve.{{val['rename']}}_{{ val['src'] }}
    {%- if val['ffill'] %}
    ,case
        when ve.{{val['rename']}}_{{ val['src'] }} is null then lag(ve.{{val['rename']}}_{{ val['src'] }},1) ignore nulls over (partition by ve.visit_id order by ve.inserted_at)
        else ve.{{val['rename']}}_{{ val['src'] }}
        end
    as {{val['rename']}}_{{ val['src'] }}_ffill
    {%- endif %}
    {%- endfor %}
    ,ve.account_id as actor_id
    ,case
        when ve.account_id = '7107d09c-0fea-4a8a-a1b9-ac3db3d77c69' then 'overseer bot'
        when ve.account_id = '157ad7d6-9d0b-4e05-a113-d6785e6278d9' then 'overseer panel'
        when ve.account_id = '8048c0fe-c824-4aac-84a8-d4f03276acd7' then 'pal cancel bot'
        when ve.account_id = 'cbb9a89e-965d-4414-830b-7f5a3d8ce18d' then 'stuck visits bot'
        when ac.permissions['admin'] = 'true' then 'admin'
        when pal_id_dac_ffill = pal.id then 'pal'
        else 'other'
        end
    as actor
    ,ve.inserted_at
    ,ve.updated_at
    ,ve.event_source
    ,ve.reason_code
    ,ve.reason_description
    from visit_events_u ve
    left join account_src ac on ve.account_id = ac.id
    left join pal_src pal on pal.account_id = ac.id
    where 1=1
    and ve.visit_id in (select * from visit_id_list)
)
,visit_events_actor_data_ffills_corrections as (
    {#  #}
    select 
    *
    {# to do: can also put this in jinja, sorta #}
    ,visit_status_dac_ffill as visit_status_after_event
    ,lag(visit_status_dac_ffill, 1) over (partition by visit_id order by inserted_at) as visit_status_at_event
    {# ,visit_status_dbc_ffill as visit_status_at_event #}
    {# ,lag(visit_status_dbc_ffill, 1) over (partition by visit_id order by inserted_at) as visit_status_after_event #}

    ,is_virtual_dac_ffill as is_virtual_after_event
    ,lag(is_virtual_dac_ffill, 1) over (partition by visit_id order by inserted_at) as is_virtual_at_event

    ,favorite_pals_only_dac_ffill as favorite_pals_only_after_event
    ,lag(favorite_pals_only_dac_ffill, 1) over (partition by visit_id order by inserted_at) as favorite_pals_only_at_event
    
    {# TO-DO consider rethinking at/after naming convention for scheduled_for ...  #}
    ,lag(scheduled_for_dac_ffill, 1) over (partition by visit_id order by inserted_at) as previous_scheduled_for
    ,coalesce(previous_scheduled_for, scheduled_for_dac) as scheduled_for_at_event
    ,scheduled_for_dac_ffill as scheduled_for_after_event

    {# pal_partner_id takes into consideration partner visits and how the can be partner_handoff, reclaimed_handoff #}
    {# ,coalesce(pal_id_dac, visit_partner_id_dac) as pal_partner_id_dac #}
    ,coalesce(pal_id_dac, iff(action='partner_handoff', 'ph-' || visit_partner_id_dac, null)) as pal_partner_id_dac
    {# ,pal_id_dac_ffill as pal_id_after_event #}
    {# ,lag(pal_id_dac_ffill, 1) over (partition by visit_id order by inserted_at) as pal_id_at_event #}
    {# ,lag(pal_id_dac_ffill, 1) over (partition by visit_id order by inserted_at) as pal_id_at_event #}
    ,case
        when pal_partner_id_dac is null then lag(pal_partner_id_dac,1) ignore nulls over (partition by visit_id order by inserted_at)
        else pal_partner_id_dac
        end
    as pal_partner_id_dac_lag
    

    {# required vehicle is tweaked as well #}
    ,case 
        when not equal_null(required_vehicle_type_dac, required_vehicle_type_dbc) and required_vehicle_type_dac is not null then 'Transpo' 
        when not equal_null(required_vehicle_type_dac, required_vehicle_type_dbc) and required_vehicle_type_dac is null then 'Non-Transpo' 
        else null
    end as transpo_dac
    ,coalesce(
        case
        when transpo_dac is null then lag(transpo_dac,1) ignore nulls over (partition by visit_id order by inserted_at)
        else transpo_dac
        end
        ,'Non-Transpo'
    ) as transpo_after_event -- transpo_dac_ffill
    from visit_events_actor_data
)
,visit_events_actor_data_ffills_corrections_previous_pal as (
    select 
    *
    ,lag(pal_partner_id_dac_lag, 1) over (partition by visit_id order by inserted_at) as pal_partner_id_dac_2xlag
    ,lag(transpo_after_event, 1) over (partition by visit_id order by inserted_at) as transpo_at_event
    ,coalesce(
        lead(inserted_at,1) over (partition by visit_id order by inserted_at)
        ,sysdate()+interval '100 years'
    ) as event_valid_until
    from visit_events_actor_data_ffills_corrections
)

,visit_events_data_ranking as (
    select
    *
    ,case 
        when 
        (action='inserted' or equal_null(scheduled_for_at_event,scheduled_for_after_event)=FALSE)
        then row_number() over (partition by visit_id order by inserted_at asc) 
        else null
        end 
    as scheduled_rank_pre
    ,case 
        when 
        equal_null(pal_partner_id_dac_2xlag,pal_partner_id_dac_lag)=FALSE
        or (equal_null(pal_partner_id_dac_2xlag,pal_partner_id_dac_lag)=TRUE and visit_status_at_event='pending' and visit_status_after_event='accepted')
        then row_number() over (partition by visit_id order by inserted_at asc) 
        else null
        end 
    as pal_rank_pre
    from visit_events_actor_data_ffills_corrections_previous_pal
)

,coalesce_ranks as (
    select
    *

    ,coalesce(
        scheduled_rank_pre
        ,lag(scheduled_rank_pre) ignore nulls over (partition by visit_id order by inserted_at)
    ) as scheduled_rank_pre_coalesced
    ,coalesce(
        pal_rank_pre
        ,lag(pal_rank_pre) ignore nulls over (partition by visit_id order by inserted_at)
    ) as pal_rank_pre_coalesced
    from visit_events_data_ranking
)
,dense_ranked_ranks as (
    select
    *
    ,case 
        when scheduled_rank_pre_coalesced is null then 0 
        else dense_rank() over (partition by visit_id order by scheduled_rank_pre_coalesced)
        end
    as scheduled_rank
    ,case 
        when pal_rank_pre_coalesced is null then 0 
        else dense_rank() over (partition by visit_id order by pal_rank_pre_coalesced)
        end
    as pal_rank
    ,coalesce(pal_id_dac, pal_id_dbc) as pal_id_coalesced
    from coalesce_ranks
)
,pre_vpfe as (
    select 
    *
    {# the two case when s that follow are to take into account the PP pals can reschedule a visit before accepting it (while it is in pending), and would otherwise have a null pal_id and be in the wrong pal flow rank #}
    ,case 
        when action in ('rescheduled', 'visit_reschedule_ivr') and  actor='other' and visit_status_at_event='pending' then pal_rank+1 
        else pal_rank end 
    as pal_change_flow_rank
    {# ,case when action in ('rescheduled', 'visit_reschedule_ivr') and  actor='other' and visit_status_at_event='pending' then null else pal_partner_id_dac_lag end as pal_id_at_event  #}
    ,coalesce(
        pal_partner_id_dac_lag, 
        lead(pal_partner_id_dac_lag) ignore nulls over (partition by visit_id, pal_change_flow_rank order by inserted_at)
    ) as pal_partner_id_of_flow -- look back and forward for fill (coalesce a lagged and a lead)
    ,case 
        when action='partner_handoff' then null 
        else coalesce(pal_id_coalesced, lag(pal_id_coalesced) ignore nulls over (partition by visit_id, pal_change_flow_rank order by inserted_at)) end 
    as pal_id_of_event
    ,datediff(seconds, inserted_at, scheduled_for_at_event)/(60*60) as hrs_event_to_scheduled_for_at_event
    from dense_ranked_ranks
)
{# -- visit events with extra steps :p #}
,visit_pal_flow_events as (
    select
    visit_id
    ,event_id
    ,pal_change_flow_rank 
    ,case when left(pal_partner_id_of_flow, 2)='ph' then null else pal_partner_id_of_flow end as pal_id_of_flow
    ,case when left(pal_partner_id_of_flow, 2)='ph' then TRUE when pal_partner_id_of_flow is null then null else FALSE end as is_partner_handoff_flow
    ,case when left(pal_partner_id_of_flow, 2)='ph' then null else pal_id_of_event end as pal_id_of_event
    -- ,pal_id_of_event
    ,action
    ,actor
    ,actor_id
    ,event_source
    ,reason_code as event_reason_code
    ,reason_description as event_reason_description
    ,visit_status_at_event
    ,visit_status_after_event
    ,transpo_at_event
    ,transpo_after_event
    ,favorite_pals_only_at_event
    ,favorite_pals_only_after_event
    ,is_virtual_at_event
    ,is_virtual_after_event
    ,scheduled_for_at_event
    ,scheduled_for_after_event
    ,scheduled_rank
    ,hrs_event_to_scheduled_for_at_event
    ,inserted_at as event_inserted_at
    ,updated_at as event_updated_at
    ,event_valid_until
    from pre_vpfe
)
{# include pal_snapshots (extra 50secs to run ?) and ffill #}
,vpfe_lj_pal_snapshots as (
    select 
    vpfe.* 
    ,coalesce(ps.vehicle, 'none') as vehicle_at_event -- should have no nulls. --  could rename to pal_vehicle_at_event
    ,pb.priority_bonus_at_event 
    from visit_pal_flow_events vpfe
    left join {{ source('analytics_snapshots', 'PAL_SNAPSHOT')}} ps 
        on vpfe.pal_id_of_flow=ps.id 
        and (vpfe.event_inserted_at between dbt_valid_from and coalesce(dbt_valid_to, sysdate()))
    LEFT JOIN  priority_bonus pb on 
                pb.visit_id = vpfe.visit_id              AND 
                vpfe.event_inserted_at between pb.inserted_at AND pb.valid_until
)

select * from vpfe_lj_pal_snapshots
order by visit_id, event_inserted_at 

