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
{{ task_id | to_var }} = dataproc_operator.DataProcHadoopOperator(
    task_id='{{ task_id | python_escape }}',
    trigger_rule='{{ trigger_rule | python_escape }}',
    main_class=CONFIGURATION_PROPERTIES['hadoop_main_class'],
    arguments=[
        "{{ '{{' }} params['mapreduce.input.fileinputformat.inputdir'] {{ '}}' }}",
        "{{ '{{' }} params['mapreduce.output.fileoutputformat.outputdir'] {{ '}}' }}"
    ],
    {% if hdfs_files %}files=[
        {% for file in hdfs_files %}
            '{{file | python_escape }}',
        {% endfor %}],
    {% endif %}
    {% if hdfs_archives %}archives=[
        {% for archive in hdfs_archives %}
            '{{ archive | python_escape }}',
        {% endfor %}],
    {% endif %}
    cluster_name=CONFIGURATION_PROPERTIES['dataproc_cluster'],
    dataproc_hadoop_properties={% include "property_set.tpl" %}.job_properties_merged,
    dataproc_hadoop_jars=CONFIGURATION_PROPERTIES['hadoop_jars'].split(','),
    gcp_conn_id=CONFIGURATION_PROPERTIES['gcp_conn_id'],
    region=CONFIGURATION_PROPERTIES['gcp_region'],
    dataproc_job_id='{{ task_id | python_escape }}',
    params={% include "property_set.tpl" %},
)
