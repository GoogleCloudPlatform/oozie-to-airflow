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

{% for dependency in dependencies %}
{{ dependency }}
{% endfor %}

from o2a.o2a_libs import functions

CONFIG={{ config | to_python }}

JOB_PROPS={{ job_properties | to_python }}

TASK_MAP={{ task_map | to_python }}

with models.DAG(
    {{ dag_name | to_python }},
    schedule_interval={% if schedule_interval %}datetime.timedelta(days={{ schedule_interval }}){% else %}None{% endif %},  # Change to suit your needs
    start_date=dates.days_ago({{ start_days_ago }}),  # Change to suit your needs
    user_defined_macros={"functions": functions, "task_map": TASK_MAP}
) as dag:

{% filter indent(4, True) %}
{% include "dag_body.tpl" %}
{% endfilter %}
