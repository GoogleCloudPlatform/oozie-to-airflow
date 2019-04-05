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


{{ task_id }} = dummy_operator.DummyOperator(task_id='{{ task_id }}', trigger_rule='{{ trigger_rule }}')
{{ task_id }}_delete_dummy = dummy_operator.DummyOperator(task_id='{{ task_id }}_delete_dummy')
{{ task_id }}_mkdir_dummy = dummy_operator.DummyOperator(task_id='{{ task_id }}_mkdir_dummy')

{{ task_id }}_delete_dummy.set_downstream({{ task_id }}_mkdir_dummy)
{{ task_id }}_mkdir_dummy.set_downstream({{ task_id }}_reorder)
{{ task_id }}.set_downstream({{ task_id }}_delete_dummy)

{%- for path in delete_paths %}
{{ task_id }}_delete{{ loop.index }} = bash_operator.BashOperator(
    task_id='{{ task_id }}_delete{{ loop.index }}',
    bash_command='hadoop fs -rm -r {{ path }}'
)
{{ task_id }}_delete_dummy.set_downstream({{ task_id }}_delete{{ loop.index }})
{{ task_id }}_delete{{ loop.index }}.set_downstream({{ task_id }}_mkdir_dummy)
{% endfor %}


{%- for path in mkdir_paths %}
{{ task_id }}_mkdir{{ loop.index }} = bash_operator.BashOperator(
    task_id='{{ task_id }}_mkdir{{ loop.index }}',
    bash_command='hadoop fs -mkdir {{ path }}'
)
{{ task_id }}_mkdir_dummy.set_downstream({{ task_id }}_mkdir{{ loop.index }})
{{ task_id }}_mkdir{{ loop.index }}.set_downstream({{ task_id }}_reorder)
{% endfor %}
