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

{{ task_id | to_var }} = bash_operator.BashOperator(
    task_id={{ task_id | python_escape_string }},
    trigger_rule={{ trigger_rule | python_escape_string }},
{#
 In BashOperator in Composer we can't read from $DAGS_FOLDER (~/dags) - permission denied.
 However we can read from ~/data -> /home/airflow/gcs/data.
 The easiest way to access it is using the $DAGS_FOLDER env variable.
#}
    bash_command="$DAGS_FOLDER/../data/prepare.sh "
                 "-c {{ '{{' }} params.configuration_properties['dataproc_cluster'] {{ '}}' }}"
                 "-r {{ '{{' }} params.configuration_properties['gcp_region'] {{ '}}' }}"
                 {% if delete is not none %}'-d %s'{% endif %}
                 {% if mkdir is not none %}'-m %s'{% endif %} \
                 % (
                     {% if delete is not none %}shlex.quote({{ delete | python_escape_string }}),{% endif %}
                     {% if mkdir is not none %}shlex.quote({{ mkdir | python_escape_string }}),{% endif %}
                 ),
    params={% include "property_set.tpl" %},
)
