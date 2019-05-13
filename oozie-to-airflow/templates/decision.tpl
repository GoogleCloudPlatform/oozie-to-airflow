{#
  Copyright 2019 Google LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 #}

def {{ task_variable_name }}_decision():
{% for key, val in case_dict.items() -%}
{%- if loop.first %}
    if f'{{ key }}' == 'True':
        return f'{{ val }}'
{% endif %}
{%- if not loop.first and not loop.last %}
    elif f'{{ key }}' == 'True':
        return f'{{ val }}'
{% endif %}
{%- if loop.last %}
    return f'{{ val }}'
{%- endif %}
{%- endfor %}


{{ task_variable_name }} = python_operator.BranchPythonOperator(
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule}}',
    python_callable={{ task_variable_name }}_decision,
)
