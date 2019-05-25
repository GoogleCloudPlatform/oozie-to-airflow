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

JOB_PROPERTIES={
   {% for key, value in job_properties.items() %}
       '{{key | python_escape}}': {% if value is none %}None{% else %}'{{ value | python_escape }}'{% endif %},
   {% endfor %}
}
CONFIGURATION_PROPERTIES={
   {% for key, value in configuration_properties.items() %}
       '{{key | python_escape}}': {% if value is none %}None{% else %}'{{ value | python_escape }}'{% endif %},
   {% endfor %}
}

with models.DAG(
    '{{ dag_name | python_escape }}',
    schedule_interval={% if schedule_interval %}datetime.timedelta(days={{ schedule_interval }}){% else %}None{% endif %},  # Change to suit your needs
    start_date=dates.days_ago({{ start_days_ago }})  # Change to suit your needs
) as dag:

{% filter indent(4, True) %}
{% include "dag_body.tpl" %}
{% endfilter %}
