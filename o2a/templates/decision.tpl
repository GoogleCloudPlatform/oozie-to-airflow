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
def {{ task_id | to_var }}_decision():
{% for key, val in case_dict.items() %}
{% if loop.first %}
    if {{ key }}:
        return "{{ val }}"
{% endif %}
{% if not loop.first and not loop.last %}
    elif {{ key }}:
        return "{{ val }}"
{% endif %}
{% if loop.last %}
    else:
        return "{{ val }}"
{% endif %}
{% endfor %}


{{ task_id | to_var }} = python_operator.BranchPythonOperator(
    task_id={{ task_id | python_escape_string }},
    trigger_rule={{ trigger_rule | python_escape_string }},
    python_callable={{ task_id | to_var }}_decision,
)
