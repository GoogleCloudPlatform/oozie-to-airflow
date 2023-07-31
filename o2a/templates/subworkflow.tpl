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

CONFIG={{ config | to_python }}

JOB_PROPS={{ job_properties | to_python }}

def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, user_defined_macros):
    with models.DAG(
        '{0}.{1}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,  # Change to suit your needs
        start_date=start_date,  # Change to suit your needs
        user_defined_macros=user_defined_macros  # Change to suit your needs
    ) as dag:

    {% filter indent(8, True) %}
    {% include "dag_body.tpl" %}
    {% endfilter %}

    return dag
