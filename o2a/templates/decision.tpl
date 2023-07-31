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
def {{ task_id | to_var }}_decision(*args, **kwargs):
{% filter indent(4, True) %}
task = kwargs.get('task')
decisions = {{ case_dict | to_python }}
for (predicate, decision) in decisions.items():
{% filter indent(4, True) %}
value = task.render_template(content=predicate, context=TEMPLATE_ENV)
if value in ("true", "True", 1):
    return decision
{% endfilter %}
return {{default_case | to_python}}
{% endfilter %}


{{ task_id | to_var }} = python.BranchPythonOperator(
    task_id={{ task_id | to_python }},
    trigger_rule={{ trigger_rule | to_python }},
    python_callable={{ task_id | to_var }}_decision,
)
