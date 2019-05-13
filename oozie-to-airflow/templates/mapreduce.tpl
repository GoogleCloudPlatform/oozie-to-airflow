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

{{ task_variable_name }} = dataproc_operator.DataProcHadoopOperator(
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
    main_class=CTX['hadoop_main_class'],
    arguments=[
        CTX['mapreduce.input.fileinputformat.inputdir'],
        CTX['mapreduce.output.fileoutputformat.outputdir']
    ],
    {% if hdfs_files %}
    files=[{% for file in hdfs_files %}f'{{ file }}', {% endfor %}],
    {% endif %}
    {% if hdfs_archives %}
    archives=[{% for archive in hdfs_archives %}f'{{ archive }}', {% endfor %}],
    {% endif %}
    cluster_name=CTX['dataproc_cluster'],
    dataproc_hadoop_properties={{ '{' }}{% for property, value in properties.items() %}'{{ property }}': f'{{value}}', {% endfor %}{{ '}' }},
    dataproc_hadoop_jars=CTX['hadoop_jars'].split(','),
    gcp_conn_id=CTX['gcp_conn_id'],
    region=CTX['gcp_region'],
    dataproc_job_id=f'{{ task_id }}'
)
