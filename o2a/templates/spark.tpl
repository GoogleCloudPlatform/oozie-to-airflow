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
{{ task_id | to_var }} = dataproc_operator.DataProcSparkOperator(
    task_id='{{ task_id | python_escape }}',
    trigger_rule='{{ trigger_rule | python_escape }}',
    {% if main_jar %}main_jar='{{ main_jar | python_escape }}',{% endif %}
    {% if main_class %}main_class='{{ main_class | python_escape }}',{% endif %}
    arguments=[
        {% for argument in arguments %}
            '{{ argument | python_escape}}',
        {% endfor %}
    ],
    {% if hdfs_files %}
        files=[
            {% for file in hdfs_files %}
                '{{ file | python_escape }}',
            {% endfor %}
        ],
    {% endif %}
    {% if hdfs_archives %}
        archives=[
            {% for archive in hdfs_archives %}
                '{{ archive | python_escape }}',
            {% endfor %}
        ],r
    {% endif %}
    job_name='{{ job_name | python_escape }}',
    cluster_name=CONFIGURATION_PROPERTIES['dataproc_cluster'],
    {% if dataproc_spark_jars %}
        dataproc_spark_jars=[
            {% for jar in dataproc_spark_jars %}
                '{{ jar | python_escape }}',
            {% endfor %}
        ],
    {% endif %}
    {% if spark_opts %}
        dataproc_spark_properties={
            {% for key, value in spark_opts.items() %}
                '{{key | python_escape}}': {% if value is none %}None{% else %}'{{ value | python_escape }}'{% endif %},
            {% endfor %}
        },
    {% endif %}
    gcp_conn_id=CONFIGURATION_PROPERTIES['gcp_conn_id'],
    region=CONFIGURATION_PROPERTIES['gcp_region'],
    params={% include "property_set.tpl" %},
)
