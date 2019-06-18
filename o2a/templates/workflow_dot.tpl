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
digraph {
    label="{{ dag_name }}";
    rankdir="LR";
    {% for node in nodes %}
        subgraph cluster_{{ node.name | to_var }} {
            label="{{ node.name }}"
            {% for task in node.tasks %}
                {{ task.task_id | to_var }}
                [shape=none]
                [label=<
                    <table border='0' cellborder='1' cellspacing="0">
                        <tr>
                            <td align="left">Task ID</td>
                            <td align="left">{{ task.task_id }}</td>
                        </tr>
                        <tr>
                            <td align="left">Template</td>
                            <td align="left">{{ task.template_name }}</td>
                        </tr>
                        <tr>
                            <td align="left">Trigger rule</td>
                            <td align="left">{{ task.trigger_rule }}</td>
                        </tr>
                    </table>
                >]
            {% endfor %}
            {% for relation in node.relations %}
                {{ relation.from_task_id | to_var }} -> {{ relation.to_task_id | to_var }}
            {% endfor %}
        }
    {% endfor %}

    {% for relation in relations %}
        {{ relation.from_task_id | to_var }} -> {{ relation.to_task_id | to_var }}
    {% endfor %}
}
