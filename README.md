<!--
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
 -->

# Oozie to Airflow

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/oozie-to-airflow.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/cloud-composer)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/oozie-to-airflow/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/oozie-to-airflow)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Updates](https://pyup.io/repos/github/GoogleCloudPlatform/oozie-to-airflow/shield.svg)](https://pyup.io/repos/github/GoogleCloudPlatform/oozie-to-airflow/)
[![Python 3](https://pyup.io/repos/github/GoogleCloudPlatform/oozie-to-airflow/python-3-shield.svg)](https://pyup.io/repos/github/GoogleCloudPlatform/oozie-to-airflow/)

A tool to easily convert between [Apache Oozie](http://oozie.apache.org/) workflows
and [Apache Airflow](https://airflow.apache.org) workflows.

The program targets Apache Airflow >= 1.10 and Apache Oozie 1.0 XML schema.

If you want to contribute to the project, please take a look at [CONTRIBUTING.md](CONTRIBUTING.md)

# Table of Contents

* [Oozie to Airflow](#oozie-to-airflow)
* [Table of Contents](#table-of-contents)
  * [Background](#background)
* [Running the Program](#running-the-program)
* [Installing from PyPi](#installing-from-pypi)
  * [Installing from the sources](#installing-from-the-sources)
  * [Running the conversion](#running-the-conversion)
  * [Structure of the application folder](#structure-of-the-application-folder)
* [Supported Oozie features](#supported-oozie-features)
  * [Control nodes](#control-nodes)
    * [Fork](#fork)
    * [Join](#join)
    * [Decision](#decision)
    * [Start](#start)
    * [End](#end)
    * [Kill](#kill)
  * [Known Limitations](#known-limitations)
  * [EL Functions](#el-functions)
* [Examples](#examples)
  * [Demo Example](#demo-example)
    * [Current limitations](#current-limitations)
    * [Output](#output)
  * [Childwf Example](#childwf-example)
    * [Output](#output-1)
    * [Current limitations](#current-limitations-1)
  * [SSH Example](#ssh-example)
    * [Output](#output-2)
    * [Current limitations](#current-limitations-2)
  * [MapReduce Example](#mapreduce-example)
    * [Output](#output-3)
    * [Current limitations](#current-limitations-3)
  * [FS Example](#fs-example)
    * [Output](#output-4)
    * [Current limitations](#current-limitations-4)
  * [Pig Example](#pig-example)
    * [Output](#output-5)
    * [Current limitations](#current-limitations-5)
  * [Shell Example](#shell-example)
    * [Output](#output-6)
    * [Current limitations](#current-limitations-6)
  * [Spark Example](#spark-example)
    * [Output](#output-7)
    * [Current limitations](#current-limitations-7)
  * [Sub\-workflow Example](#sub-workflow-example)
    * [Output](#output-8)
    * [Current limitations](#current-limitations-8)
  * [Decision Example](#decision-example)
    * [Output](#output-9)
    * [Current limitations](#current-limitations-9)
  * [EL Example](#el-example)
    * [Output](#output-10)
    * [Current limitations](#current-limitations-10)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

## Background

Apache Airflow is a workflow management system developed by AirBnB in 2014.
It is a platform to programmatically author, schedule, and monitor workflows.
Airflow workflows are designed as [Directed Acyclic Graphs](https://airflow.apache.org/tutorial.html#example-pipeline-definition)
(DAGs) of tasks in Python. The Airflow scheduler executes your tasks on an array of
workers while following the specified dependencies.

Apache Oozie is a workflow scheduler system to manage Apache Hadoop jobs.
Oozie workflows are also designed as [Directed Acyclic Graphs](https://oozie.apache.org/docs/3.1.3-incubating/DG_Overview.html)
(DAGs) in XML.

There are a few differences noted below:


|         | Spec.  | Task        | Dependencies                    | "Subworkflows" | Parameterization             | Notification        |
|---------|--------|-------------|---------------------------------|----------------|------------------------------|---------------------|
| Oozie   | XML    | Action Node | Control Node                    | Subworkflow    | EL functions/Properties file | URL based callbacks |
| Airflow | Python | Operators   | Trigger Rules, set_downstream() | SubDag         | jinja2 and macros            | Callbacks/Emails    |


# Running the Program

Note that you need Python >= 3.6 to run the converter.

# Installing from PyPi

You can install `o2a` from PyPi via `pip install o2a`. After installation, the
[o2a](bin/o2a) and [o2a-validate-workflows](bin/o2a-validate-workflows) should be available on your path.

## Installing from the sources

In case you use sources of `o2a`, the environment can be set up via the virtualenv setup
(you can create one using [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/)
for example.

While in your virtualenv, you can install all the requirements via `pip install -r requirements.txt`.

You can add the [bin](bin) subdirectory to your
PATH, then all the scripts below can be run without adding the `./bin` prefix.
This can be done for example by adding a line similar to the one below to your `.bash_profile`
or `bin/postactivate` from your virtual environment:

```bash
export PATH=${PATH}:<INSERT_PATH_TO_YOUR_OOZIE_PROJECT>/bin
```

Otherwise you need to run all the scripts from the bin subdirectory, for example:

```bash
./bin/o2a --help
```

In all the example commands below, it is assumed that the [bin](bin) directory is in your PATH -
either installed from PyPi or from the sources.

You can also install `o2a` from local folder using `pip install -e .`

## Running the conversion

You can run the program by calling:
`o2a -i <INPUT_APPLICATION_FOLDER> -o <OUTPUT_FOLDER_PATH>`

Example:
`o2a -i examples/demo -o output/demo`

This is the full usage guide, available by running `o2a -h`

```
usage: o2a [-h] -i INPUT_DIRECTORY_PATH -o OUTPUT_DIRECTORY_PATH [-d DAG_NAME]
           [-u USER] [-s START_DAYS_AGO] [-v SCHEDULE_INTERVAL]

Convert Apache Oozie workflows to Apache Airflow workflows.

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_DIRECTORY_PATH, --input-directory-path INPUT_DIRECTORY_PATH
                        Path to input directory
  -o OUTPUT_DIRECTORY_PATH, --output-directory-path OUTPUT_DIRECTORY_PATH
                        Desired output directory
  -d DAG_NAME, --dag-name DAG_NAME
                        Desired DAG name [defaults to input directory name]
  -u USER, --user USER  The user to be used in place of all ${user.name}
                        [defaults to user who ran the conversion]
  -s START_DAYS_AGO, --start-days-ago START_DAYS_AGO
                        Desired DAG start as number of days ago
  -v SCHEDULE_INTERVAL, --schedule-interval SCHEDULE_INTERVAL
                        Desired DAG schedule interval as number of days
```

## Structure of the application folder

The input application directory has to follow the structure defined as follows:

```
<APPLICATION>/
             |- job.properties        - job properties that are used to run the job
             |- hdfs                  - folder with application - should be copied to HDFS
             |     |- workflow.xml    - Oozie workflow xml (1.0 schema)
             |     |- ...             - additional folders required to be copied to HDFS
             |- configuration.template.properties - template of configuration values used during conversion
             |- configuration.properties          - generated properties for configuration values
```


# Supported Oozie features

## Control nodes
### Fork

A [fork node](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.1.5_Fork_and_Join_Control_Nodes)
splits the path of execution into multiple concurrent paths of execution.

### Join

A [join node](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.1.5_Fork_and_Join_Control_Nodes)
waits until every concurrent execution of the previous fork node arrives to it. The fork and join nodes must be used in pairs. The join node
assumes concurrent execution paths are children of the same fork node.
~~~~
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.1">
    ...
    <fork name="[FORK-NODE-NAME]">
        <path start="[NODE-NAME]" />
        ...
        <path start="[NODE-NAME]" />
    </fork>
    ...
    <join name="[JOIN-NODE-NAME]" to="[NODE-NAME]" />
    ...
</workflow-app>
~~~~

### Decision

A [decision node](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.1.4_Decision_Control_Node)
enables a workflow to make a selection on the execution path to follow.

The behavior of a decision node can be seen as a switch-case statement.

A decision node consists of a list of predicates-transition pairs plus a default transition. Predicates are evaluated in order or appearance until one of them evaluates to true and the corresponding transition is taken. If none of the predicates evaluates to true the default transition is taken.

Predicates are JSP Expression Language (EL) expressions (refer to section 4.2 of this document) that resolve into a boolean value, true or false . For example:
`${fs:fileSize('/usr/foo/myinputdir') gt 10 * GB}`

~~~~
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.1">
    ...
    <decision name="[NODE-NAME]">
        <switch>
            <case to="[NODE_NAME]">[PREDICATE]</case>
            ...
            <case to="[NODE_NAME]">[PREDICATE]</case>
            <default to="[NODE_NAME]"/>
        </switch>
    </decision>
    ...
</workflow-app>
~~~~
### Start

The [start node](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.1.1_Start_Control_Node)
is the entry point for a workflow job, it indicates the first workflow node the workflow job must transition to.

When a workflow is started, it automatically transitions to the node specified in the start .

A workflow definition must have one start node.

~~~~
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.1">
  ...
  <start to="[NODE-NAME]"/>
  ...
</workflow-app>
~~~~
### End

The [end node](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.1.2_End_Control_Node)
is the end for a workflow job, it indicates that the workflow job has completed successfully.

When a workflow job reaches the end it finishes successfully (SUCCEEDED).

If one or more actions started by the workflow job are executing when the end node is reached, the actions will be killed. In this scenario the workflow job is still considered as successfully run.

A workflow definition must have one end node.

~~~~
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.1">
    ...
    <end name="[NODE-NAME]"/>
    ...
</workflow-app>
~~~~

### Kill

The [kill node](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.1.3_Kill_Control_Node)
allows a workflow job to exit with an error.

When a workflow job reaches the kill it finishes in error (KILLED).

If one or more actions started by the workflow job are executing when the kill node is reached, the actions will be killed.

A workflow definition may have zero or more kill nodes.

~~~~
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.1">
    ...
    <kill name="[NODE-NAME]">
        <message>[MESSAGE-TO-LOG]</message>
    </kill>
    ...
</workflow-app>
~~~~

## Known Limitations

The goal of this program is to mimic both the actions and control flow
that is outlined by the Oozie workflow file. Unfortunately there are some
limitations as of now that have not been worked around regarding the execution
flow. The situation where the execution path might not execute correctly is when
there are 4 nodes, A, B, C, D, with the following Oozie specified execution paths:
```
A executes ok to C
B executes error to C

A executes error to D
B executes ok to D
```
In this situation Airflow does not have enough fine grained node execution control.
The converter should be able to handle this situation in the future, but it is not
currently guaranteed to work.

This is because if goes from A to C on ok, and B goes to C on error, C's trigger rule
will have to be set to `DUMMY`, but this means that if A goes to error, and B goes to ok
C will then execute incorrectly.

This limitation is temporary and will be removed in a future version of Oozie-2-Airflow converter.


## EL Functions

As of now, a very minimal set of [Oozie EL](https://oozie.apache.org/docs/4.0.1/WorkflowFunctionalSpec.html#a4.2_Expression_Language_Functions)
functions are supported. The way they work is that there exists a
dictionary mapping from each Oozie EL function string to the
corresponding Python function. This is in `utils/el_utils.py`.
This design allows for custom EL function mapping if one so chooses. By
default everything gets mapped to the module `o2a_libs`. This means in
order to use EL function mapping, the folder `o2a_libs` should
be copied over to the Airflow DAG folder. This should then be picked up and
parsed by the Airflow workers and then available to all DAGs.

# Examples

All examples can be found in the [examples](examples) directory.

* [Demo](#demo-example)
* [SSH](#ssh-example)
* [MapReduce](#mapreduce-example)
* [Pig](#pig-example)
* [Shell](#shell-example)
* [Sub-workflow](#sub-workflow-example)
* [Decision](#decision-example)
* [EL](#el-example)

## Demo Example

The demo example contains several action and control nodes. The control
nodes are `fork`, `join`, `decision`, `start`, `end`, and `kill`. As far as action
nodes go, there are `fs`, `map-reduce`, and `pig`.

Most of these are already supported, but when the program encounters a node it does
not know how to parse, it will perform a sort of "skeleton transformation" -
it will convert all the unknown nodes to dummy nodes. This will
allow users to manually parse the nodes if they so wish as the control flow
is there.

The demo can be run as:

`o2a -i examples/demo -o output/demo`

This will parse and write to an output file in the `output/demo` directory.

### Current limitations

The decision node is not fully functional as there is not currently
support for all EL functions. So in order for it to run in Airflow you must
edit the Python output file and change the decision node expression.

### Output
In this example the output will appear in `/output/ssh/test_demo_dag.py`.
Additionally subworkflow is generated in  `/output/ssh/subdag_test.py`.

## Childwf Example

The childwf example is sub-workflow for the `demo` example. It can be run as:

`o2a -i examples/childwf -o output/childwf`

Make sure to first copy `examples/subwf/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `output/childwf/test_childwf_dag.py`.

### Current limitations

No known limitations.

## SSH Example

The ssh example can be run as:

`o2a -i examples/ssh -o output/ssh`

This will convert the specified Oozie XML and write the output into the
specified output directory, in this case `output/ssh/test_ssh_dag.py`.

There are some differences between Apache Oozie and Apache Airflow as far as the SSH specification goes.
In Airflow you will have to add/edit an SSH-specific connection that contains
the credentials required for the specified SSH action. For example, if
the SSH node looks like:
```xml
<action name="ssh">
    <ssh xmlns="uri:oozie:ssh-action:0.1">
        <host>user@apache.org</host>
        <command>echo</command>
        <args>"Hello Oozie!"</args>
    </ssh>
    <ok to="end"/>
    <error to="fail"/>
</action>
```
Then the default Airflow SSH connection, `ssh_default` should have at
the very least a password set. This can be found in the Airflow Web UI
under **Admin > Connections**. From the command line it is impossible to
edit connections so you must add one like:

`airflow connections --add --conn_id <SSH_CONN_ID> --conn_type SSH --conn_password <PASSWORD>`

More information can be found in [Airflow's documentation](https://airflow.apache.org/cli.html#connections).

### Output
In this example the output will appear in `/output/ssh/test_ssh_dag.py`.

The converted DAG uses the `SSHOperator` in Airflow.

### Current limitations

No known limitations.

## MapReduce Example

The MapReduce example can be run as:

`o2a -i examples/mapreduce -o output/mapreduce`

Make sure to first copy `examples/mapreduce/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `/output/mapreduce/test_mapreduce_dag.py`.

The converted DAG uses the `DataProcHadoopOperator` in Airflow.

### Current limitations

**1. Exit status not available**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.2.2_Map-Reduce_Action):
> The counters of the Hadoop job and job exit status (FAILED, KILLED or SUCCEEDED) must be available to the
workflow job after the Hadoop jobs ends. This information can be used from within decision nodes and other
actions configurations.

Currently we use the `DataProcHadoopOperator` which does not store the job exit status in an XCOM for other tasks to use.

**2. Configuration options**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.2.2_Map-Reduce_Action)
(the strikethrough is from us):
> Hadoop JobConf properties can be specified as part of
> - ~~the config-default.xml or~~
> - ~~JobConf XML file bundled with the workflow application or~~
> - ~~\<global> tag in workflow definition or~~
> - Inline map-reduce action configuration or
> - ~~An implementation of OozieActionConfigurator specified by the <config-class> tag in workflow definition.~~

Currently the only supported way of configuring the map-reduce action is with the
inline action configuration, i.e. using the `<configuration>` tag in the workflow's XML file definition.

**3. Streaming and pipes**

Streaming and pipes are currently not supported.

## FS Example

The FS example can be run as:

`o2a -i examples/fs -o output/fs`

Make sure to first copy `examples/fs/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `/output/fs/test_fs_dag.py`.

The converted DAG uses the `BashOperator` in Airflow.

### Current limitations

Not all FS operations are currently idempotent. This will be fixed.

## Pig Example

The Pig example can be run as:

`o2a -i examples/pig -o output/pig`

Make sure to first copy `examples/pig/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `output/pig/test_pig_dag.py`.

The converted DAG uses the `DataProcPigOperator` in Airflow.

### Current limitations

**1. Configuration options**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.2.3_Pig_Action)
(the strikethrough is from us):
> Hadoop JobConf properties can be specified as part of
> - ~~the config-default.xml or~~
> - ~~JobConf XML file bundled with the workflow application or~~
> - ~~\<global> tag in workflow definition or~~
> - Inline pig action configuration.

Currently the only supported way of configuring the pig action is with the
inline action configuration, i.e. using the `<configuration>` tag in the workflow's XML file definition.

## Shell Example

The Shell example can be run as:

`o2a -i examples/shell -o output/shell`

Make sure to first copy `examples/shell/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `output/shell/test_shell_dag.py`.

The converted DAG uses the `BashOperator` in Airflow, which executes the desired shell
action with Pig by invoking `gcloud dataproc jobs submit pig --cluster=<cluster> --region=<region>
--execute 'sh <action> <args>'`.

### Current limitations

**1. Exit status not available**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/DG_ShellActionExtension.html):
> The output (STDOUT) of the Shell job can be made available to the workflow job after the Shell job ends.
This information could be used from within decision nodes.

Currently we use the `BashOperator` which can store only the last line of the job output in an XCOM.
In this case the line is not helpful as it relates to the Dataproc job submission status and
not the Shell action's result.

**2. No Shell launcher configuration**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/DG_ShellActionExtension.html):
> Shell launcher configuration can be specified with a file, using the job-xml element, and inline,
using the configuration elements.

Currently there is no way specify the shell launcher configuration (it is ignored).

## Spark Example

The Shell example can be run as:

`o2a -i examples/spark -o output/spark`

Make sure to first copy `/examples/spark/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `/output/spark/spark.py`.

The converted DAG uses the `DataProcSparkOperator` in Airflow.

### Current limitations

**1. Ony tasks written in Java are supported**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/DG_ShellActionExtension.html):
> The jar element indicates a comma separated list of jars or python files.

The solution was tested with only a single Jar file.

**2. No Spark launcher configuration**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/DG_SparkActionExtension.html):
> Shell launcher configuration can be specified with a file, using the job-xml element, and inline,
using the configuration elements.

Currently there is no way to specify the Spark launcher configuration (it is ignored).

**3. Not all elements are supported**

The following elements are not supported: `job-tracker`, `name-node`, `master`, `mode`.

## Sub-workflow Example

The Sub-workflow example can be run as:

`o2a -i examples/subwf -o output/subwf`

Make sure to first copy `examples/subwf/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `output/subwf/test_subwf_dag.py`.
Additionally, a `subdag_test.py` (name to be changed soon) file is generated in the same directory,
which contains the factory method `sub_dag()` returning the actual Airflow subdag.

The converted DAG uses the `SubDagOperator` in Airflow.

### Current limitations

Currently generated name of the sub-workflow is fixed which means that only one subworkflow is supported
per DAG folder. This will be fixed soon.

## Decision Example

The decision example can be run as:

`o2a -i examples/decision -o output/decision`

Make sure to first copy `examples/decision/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will appear in `output/decision/test_decision_dag.py`.

The converted DAG uses the `BranchPythonOperator` in Airflow.

### Current limitations

Decision example is not yet fully functional as EL functions are not yet fully implemented so condition is
hard-coded for now. Once EL functions are implemented, the condition in the example will be updated.

## EL Example

The Oozie Expression Language (EL) example can be run as:
`o2a -i examples/el -o output/el`

This will showcase the ability to use the `o2a_libs` directory to map EL functions
to Python methods. This example assumes that the user has a valid Apache Airflow
SSH connection set up and the `o2a_libs` directory has been copied to the dags
folder.

Please keep in mind that as of the current version only a single EL variable
or single EL function. Variable/function chaining is not currently supported.

### Output
In this example the output will appear in `output/el/test_el_dag.py`.

### Current limitations

Decision example is not yet fully functional as EL functions are not yet fully implemented so condition is
hard-coded for now. Once EL functions are implemented, the condition in the example will be updated.
