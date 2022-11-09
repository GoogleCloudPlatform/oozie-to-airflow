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
{{ task_id | to_var }} = spark_submit_operator.SparkSubmitOperator(
    task_id={{ task_id | to_python }},
    application='',
    {% if conf %}
        conf={{ conf | to_python }},
    {% endif %}
    conn_id='spark_default',
    {% if files %}
        files={{ files | to_python }},
    {% endif %}
    {% if jars %}
        jars={{ jars | to_python }},
    {% endif %}
    {% if main_class %}java_class={{ main_class | to_python }},{% endif %}
    {% if executor_cores %}executor_cores  ={{ executor_cores }},{% endif %}
    {% if executor_memory %}executor_memory ={{ executor_memory  | to_python }},{% endif %}
    {% if driver_memory %}driver_memory={{ driver_memory | to_python }},{% endif %}
    {% if num_executors %}num_executors={{ num_executors }},{% endif %}
    keytab=None,
    principal=None,
    application_args={{ arguments | to_python }},
    name={{ name | to_python }},
    trigger_rule={{ trigger_rule | to_python }},
    {% if archives %}
        archives={{ archives | to_python }}.
    {% endif %}
    params={{ props_macro.props(action_node_properties=action_node_properties) }},
)
