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


{{ task_id }} = spark_submit_operator.SparkSubmitOperator(
    task_id = '{{ task_id }}',
    trigger_rule = '{{ trigger_rule }}',
    params=PARAMS,
    # Spark specific
    name = {{ spark_name }},
    application = {{ application }},
    conf = {{ conf }},
    conn_id = 'spark_default',
    files = {{ files }},
    py_files = {{ py_files }},
    jars = {{ jars }},
    java_class = {{ java_class }},
    packages = {{ packages }},
    exclude_packages = {{ exclude_packages }},
    repositories = {{ repositories }},
    total_executor_cores = {{ total_executor_cores }},
    executor_cores = {{ executor_cores }},
    executor_memory = {{ executor_memory }},
    driver_memory = {{ driver_memory }},
    keytab = {{ keytab }},
    principal = {{ principal }},
    num_executors = {{ num_executors }},
    application_args = {{ application_args }},
    verbose = {{ verbose }},
    env_vars = {{ env_vars }},
    driver_classpath = {{ driver_classpath }},
)
