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

{% import "macros/props.tpl" as props_macro %}
{{ task_id | to_var }} = hive_operator.HiveOperator(
    task_id={{ task_id | to_python }},
    trigger_rule={{ trigger_rule | to_python }},
    hql={{ hql | to_python }},
    {% if hive_cli_conn_id %}hive_cli_conn_id={{ hive_cli_conn_id | to_python }},{% endif %}
    mapred_queue={{ mapred_queue | to_python }},
    hiveconfs={{ props_macro.props(action_node_properties=action_node_properties, xml_escaped=True) }},
    mapred_job_name={{ task_id | to_python }},
)


