{{ config(materialized='view') }}
{%- set date_start = "'2010-01-01'" %}
{%- set date_seq = 10000 %}
{%- set date_parts = [
    {'part': 'week',},
    {'part': 'month',},
    {'part': 'quarter',},
    {'part': 'year',},
] -%}

with 
date_seq as (
	select
	dateadd(day, seq4(), {{ date_start }}) as date
	from table(generator(rowcount=>{{ date_seq }}))
)
,cte as (
	select
	d.date as date
	{%- for object in date_parts %}
	, date_trunc({{ object['part'] }}, d.date) as {{ object['part'] }}_date
	{%- endfor %}
	, day(d.date) as day_of_month
	, dayname(d.date) as day_of_week
	, weekofyear(d.date) as week_of_year
	, month(d.date) as month
	, monthname(d.date) as month_name
	, quarter(d.date) as quarter
	, year(d.date) as year
	from date_seq d
)

select * from cte
