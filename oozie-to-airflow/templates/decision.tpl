def {{ task_id }}_decision():
{%- for key, val in case_dict -%}
{%- if loop.first %} 
    if {{ key }}:
        return {{ val }}
{% endif %} 
{%- if not loop.first and not loop.last %}
    elif {{ key }}:
        return {{ val }}
{% endif %} 
{%- if loop.last %} 
    else:
        return {{ val }}
{%- endif %} 
{%- endfor %}

{{ task_id }} = python_operator.BranchPythonOperator(
    python_callable={{task_id}}_decision,
    task_id='{{task_id}}',
    trigger_rule='{{trigger_rule}}',
)

