-- refactor of a very non-DRY (wet?) modeling of Jira data. Used jinja, json and loops to create a compact model without a join per field 
{%- set customfields_dicts = [
    { 'field_id': 'customfield_10050', 'type': 'name', 'field_name': 'client_name' },
    { 'field_id': 'customfield_10157', 'type': 'name', 'field_name': 'client_member_complaint_name' },
    { 'field_id': 'customfield_10247', 'type': 'name', 'field_name': 'program_name' },
    { 'field_id': 'customfield_10308', 'type': 'name', 'field_name': 'source_referrals' },
    { 'field_id': 'customfield_10326', 'type': 'name', 'field_name': 'send_to_client' },
    { 'field_id': 'customfield_10333', 'type': 'name', 'field_name': 'member_complaint_filed_on_behalf_of' },
    { 'field_id': 'customfield_10062', 'type': 'value', 'field_name': 'incident_date_time', 'cast': 'timestamp'},
    { 'field_id': 'customfield_10052', 'type': 'value', 'field_name': 'member_id' },
    { 'field_id': 'customfield_10051', 'type': 'value', 'field_name': 'member_name' },
    { 'field_id': 'customfield_10074', 'type': 'value', 'field_name': 'pal_name' },
    { 'field_id': 'customfield_10244', 'type': 'value', 'field_name': 'service_request_id' },
    { 'field_id': 'customfield_10095', 'type': 'value', 'field_name': 'insurance_id' },
    { 'field_id': 'customfield_10160', 'type': 'value', 'field_name': 'complaint_resolution' },
    { 'field_id': 'customfield_10124', 'type': 'value', 'field_name': 'member_phone_number' },
    { 'field_id': 'customfield_10105', 'type': 'value', 'field_name': 'member_date_of_birth' },
    { 'field_id': 'customfield_10104', 'type': 'value', 'field_name': 'member_city_state' },
    { 'field_id': 'customfield_10041', 'type': 'value', 'field_name': 'time_to_resolution' },
    { 'field_id': 'customfield_10042', 'type': 'value', 'field_name': 'time_to_first_response' },
    { 'field_id': 'customfield_10317', 'type': 'value', 'field_name': 'visit_id' },
    { 'field_id': 'customfield_10079', 'type': 'value', 'field_name': 'due_date' },
    { 'field_id': 'customfield_10010', 'type': 'value', 'field_name': 'request_type' },
    { 'field_id': 'customfield_10004', 'type': 'name', 'field_name': 'impact' },

    { 'field_id': 'timeoriginalestimate', 'type': 'value', 'field_name': 'original_estimate_seconds' },
    { 'field_id': 'timespent', 'type': 'value', 'field_name': 'time_spent_seconds' },
    { 'field_id': 'customfield_10334', 'type': 'name', 'field_name': 'complaint_warm_transfer' },
    { 'field_id': 'customfield_10245', 'type': 'name', 'field_name': 'visit_type' },
    { 'field_id': 'customfield_10290', 'type': 'name', 'field_name': 'member_visit_type' },
    ]
-%}

with
jtc_issue as (
    select * from {{ source('jira_ticket_connector', 'ISSUE') }}
)
,jtc_priority as (
    select * from {{ source('jira_ticket_connector', 'PRIORITY') }}
)
,jtc_issue_field_history as (
    select * from {{ source('jira_ticket_connector', 'ISSUE_FIELD_HISTORY') }}
)
,jtc_field_option as (
    select * from {{ source('jira_ticket_connector', 'FIELD_OPTION') }}
)
,ft_papas as (
    select * from {{ ref('papa') }}
)
,ft_demand_profiles as (
    select * from {{ ref('county') }}
)
,lj_tables as (
    select 
    i.id as issue_id
    ,trim(ifh.field_id) as field_id
    ,count(*) as issue_id_count
    ,listagg(fo.name, ' | ') as name
    ,listagg(ifh.value, ' | ') as value
    from jtc_issue i
    left join jtc_issue_field_history as ifh on ifh.issue_id = i.id
    left join jtc_field_option as fo on fo.id::text = ifh.value
    where 1=1
    and ifh.is_active = TRUE
    group by 1, 2
)
,grpby_table as (
    select
    issue_id
    ,object_agg(field_id::varchar, value::variant) as value_x_field_id_jsonblob
    ,object_agg(field_id::varchar, name::variant) as name_x_field_id_jsonblob
    {#- ,object_agg(k, v) as jsonblob -- key MUST be varchar, value MUST be variant #}
    from lj_tables
    group by 1
)

{#- -- get value from jsonblob, regexp_replace to remove double quotation marks from value #}
,dict_to_table as (
    select 
    issue_id
    {#- -- ,value_x_field_id_jsonblob[] as #}
    {#- -- ,name_x_field_id_jsonblob[] as #}
    {#- -- ,regexp_replace(jsonblob['key'], '"', '') as clean_key #}
    {%- for cf in customfields_dicts %}
    {%- if cf['cast'] %}
        ,try_cast(trim(regexp_replace({{cf['type']}}_x_field_id_jsonblob['{{ cf['field_id']}}'], '"', '')) as {{ cf['cast'] }}) as {{ cf['field_name'] }}
    {%- else %}
        ,trim(regexp_replace({{cf['type']}}_x_field_id_jsonblob['{{ cf['field_id']}}'], '"', '')) as {{ cf['field_name'] }}
    {%- endif %}
    {%- endfor %}
    from grpby_table
)
,final as (
    select 

    a.* rename (insurance_id as entity_id)
    ,p.name as priority
    ,pa.county
    ,pa.state
    ,dp.segmentation
    from dict_to_table a
    left join ft_papas pa on pa.id = trim(a.insurance_id)
    left join ft_demand_profiles dp on dp.county_state = pa.county_state
    left join jtc_issue i on i.id = a.issue_id
    left join jtc_priority p on i.priority = p.id
)

select * from final 
