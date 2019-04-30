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

{{ task_id }} = dataproc_operator.DataProcHadoopOperator(
    main_class=PARAMS['hadoop_main_class'],
    arguments=['{{ properties['mapreduce.input.fileinputformat.inputdir'] }}', '{{ properties['mapreduce.output.fileoutputformat.outputdir'] }}'],
    {% if hdfs_files %}
    files={{ hdfs_files | tojson }},
    {% endif %}
    {% if hdfs_archives %}
    archives={{ hdfs_archives | tojson }},
    {% endif %}
    cluster_name=PARAMS['dataproc_cluster'],
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
    dataproc_hadoop_properties={{ properties }},
    dataproc_hadoop_jars=PARAMS['hadoop_jars'],
    gcp_conn_id=PARAMS['gcp_conn_id'],
    region=PARAMS['gcp_region'],
    dataproc_job_id='{{ task_id }}'
)
