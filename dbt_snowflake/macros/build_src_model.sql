{% macro build_src_model(src_table, exceptions) %}
{# ['exceptions', 'have', 'to', 'be', 'a', 'list', 'of', 'strings'] or empty #}
{%- set columns = adapter.get_columns_in_relation( ref(src_table) ) %}

with 
cte as (
    select 
    * 
    from {{ ref(src_table) }} a
)

select 
{%- for col in columns if col.name|lower not in exceptions %}
{{ col.name|lower }}
{%- if not loop.last %},{% endif %}
{%- endfor %}
from cte

{% endmacro %}
