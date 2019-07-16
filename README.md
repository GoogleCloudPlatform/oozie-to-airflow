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

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/oozie-to-airflow.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/oozie-to-airflow)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/oozie-to-airflow/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/oozie-to-airflow)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=GoogleCloudPlatform/oozie-to-airflow)](https://dependabot.com)
[![Python 3](https://pyup.io/repos/github/GoogleCloudPlatform/oozie-to-airflow/python-3-shield.svg)](https://pyup.io/repos/github/GoogleCloudPlatform/oozie-to-airflow/)

A tool to easily convert between [Apache Oozie](http://oozie.apache.org/) workflows
and [Apache Airflow](https://airflow.apache.org) workflows.

The program targets Apache Airflow >= 1.10 and Apache Oozie 1.0 XML schema.

If you want to contribute to the project, please take a look at [CONTRIBUTING.md](CONTRIBUTING.md)

# Table of Contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Background](#background)
- [Running the Program](#running-the-program)
- [Installing from PyPi](#installing-from-pypi)
  - [Installing from the sources](#installing-from-the-sources)
  - [Running the conversion](#running-the-conversion)
  - [Structure of the application folder](#structure-of-the-application-folder)
- [Supported Oozie features](#supported-oozie-features)
  - [Control nodes](#control-nodes)
  - [EL Functions](#el-functions)
- [Common Known Limitations](#common-known-limitations)
  - [File/Archive functionality](#filearchive-functionality)
  - [Not all global configuration methods are supported](#not-all-global-configuration-methods-are-supported)
  - [Support for uber.jar feature](#support-for-uberjar-feature)
  - [Support for .so and .jar lib files](#support-for-so-and-jar-lib-files)
  - [Custom messages missing for Kill Node](#custom-messages-missing-for-kill-node)
  - [Capturing output is not supported](#capturing-output-is-not-supported)
  - [Subworkflow DAGs must be placed in examples](#subworkflow-dags-must-be-placed-in-examples)
- [Examples](#examples)
  - [Demo Example](#demo-example)
  - [Childwf Example](#childwf-example)
  - [SSH Example](#ssh-example)
  - [MapReduce Example](#mapreduce-example)
  - [FS Example](#fs-example)
  - [Java Example](#java-example)
  - [Pig Example](#pig-example)
  - [Shell Example](#shell-example)
  - [Spark Example](#spark-example)
  - [Sub-workflow Example](#sub-workflow-example)
  - [DistCp Example](#distcp-example)
  - [Decision Example](#decision-example)
  - [EL Example](#el-example)
  - [Hive/Hive2 Example](#hivehive2-example)
  - [Email Example](#email-example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Background

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
usage: o2a [-h] -i INPUT_DIRECTORY_PATH -o OUTPUT_DIRECTORY_PATH [-n DAG_NAME]
           [-u USER] [-s START_DAYS_AGO] [-v SCHEDULE_INTERVAL] [-d]

Convert Apache Oozie workflows to Apache Airflow workflows.

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_DIRECTORY_PATH, --input-directory-path INPUT_DIRECTORY_PATH
                        Path to input directory
  -o OUTPUT_DIRECTORY_PATH, --output-directory-path OUTPUT_DIRECTORY_PATH
                        Desired output directory
  -n DAG_NAME, --dag-name DAG_NAME
                        Desired DAG name [defaults to input directory name]
  -u USER, --user USER  The user to be used in place of all ${user.name}
                        [defaults to user who ran the conversion]
  -s START_DAYS_AGO, --start-days-ago START_DAYS_AGO
                        Desired DAG start as number of days ago
  -v SCHEDULE_INTERVAL, --schedule-interval SCHEDULE_INTERVAL
                        Desired DAG schedule interval as number of days
  -d, --dot             Renders workflow files in DOT format
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

# Common Known Limitations

There are few limitations in the implementation of the Oozie-To-Airflow converter. It's not possible to
write a converter that handles all cases of complex workflows from Ooozie because some of
functionalities available are not possible to map easily to existing Airflow Operators or
cannot be tested because of the current Dataproc + Composer limitations. Some of those limitations
might be removed in the future. Below is a list of common known limitations that we are aware of for now.

Many of those limitations are not blockers - the workflows will still be converted to Python DAGs
and it should be possible to manually (or automatically) post-process the DAGs to add custom
functionality. So even with those limitations in place you can still save a ton of work when
converting many Oozie workflows.

In the following, "Examples" section more specific per-action limitations are listed as well.

## File/Archive functionality

At the time of this writing we were not able to determine if file/archive
functionality works as intended. While we map appropriate file/archive methods it seems that Oozie
treats file/archive somewhat erraticaly. This is not a blocker to run most of the operations, however
some particular complex workflows might be problematic. Further testing with real, production Oozie
workflows is needed to verify our implementation.

* [File/Archive in Pig doesn't work](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/243)

## Not all global configuration methods are supported

Oozie implements a number of ways how configuration parameters are passed to actions. Out of the existing
configuration options the following ones are not supported (but can be easily added as needed):

* [The config-default.xml file](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/137)
* [Parameters section of workflow.xml](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/138)
* [Handle Global configuration properties](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/134)

## Support for uber.jar feature

The uber.jar feature is not supported.

* [Support uber.jar feature](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/140)

## Support for .so and .jar lib files

Oozie adds .so and .jar files from the lib folder to Local Cache for all the jobs run to
LD_LIBRARY_PATH/CLASSPATH. Currently only Java Mapper supports it.

* [Support for Oozie lib .so files](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/302)
* [Support for Oozie lib .jar files](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/301)

## Custom messages missing for Kill Node

The Kill Node might have custom log message specified. This is not implemented:
[Add handling of custom Kill Node message](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/97)

## Capturing output is not supported

In several actions you can capture output from tasks. This is not yet implemented.

* [Add support for capture-ouput](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/155)

## Subworkflow DAGs must be placed in examples

Currently all subworkflow DAGs must be in examples folder

* [Subworkflow conversion expects to be run in examples](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/213)

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

### Known limitations

The decision node is not fully functional as there is not currently
 support for all EL functions. So in order for it to run in Airflow you may need to
edit the Python output file and change the decision node expression.

Issue in GitHub: [Implement decision node](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/42)

### Output
In this example the output (including sub-workflow dag) will be created in the `./output/ssh/` folder.

## Childwf Example

The childwf example is sub-workflow for the `demo` example. It can be run as:

`o2a -i examples/childwf -o output/childwf`

Make sure to first copy `examples/subwf/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/childwf/` folder.

### Known limitations

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
In this example the output will be created in the `./output/ssh/` folder.

The converted DAG uses the `SSHOperator` in Airflow.

### Known limitations

No known limitations.

## MapReduce Example

The MapReduce example can be run as:

`o2a -i examples/mapreduce -o output/mapreduce`

Make sure to first copy `examples/mapreduce/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/mapreduce/` folder.

The converted DAG uses the `DataProcHadoopOperator` in Airflow.

### Known limitations

**1. Exit status not available**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.2.2_Map-Reduce_Action):
> The counters of the Hadoop job and job exit status (FAILED, KILLED or SUCCEEDED) must be available to the
workflow job after the Hadoop jobs ends. This information can be used from within decision nodes and other
actions configurations.

Currently we use the `DataProcHadoopOperator` which does not store the job exit status in an XCOM for other tasks to use.

Issue in Github: [Implement exit status and counters in MapReduce Action](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/337)

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

Issues in Github:
* [Add support for config-default.xml](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/137)
* [Add support for parameters section of the workflow.xml](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/138)
* [Handle global configuration properties](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/134)

**3. Streaming and pipes**

Streaming and pipes are currently not supported.

Issue in github [Implement streaming support](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/336)

## FS Example

The FS example can be run as:

`o2a -i examples/fs -o output/fs`

Make sure to first copy `examples/fs/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/fs/` folder.

The converted DAG uses the `BashOperator` in Airflow.

### Known limitations

Not all FS operations are currently idempotent. It's not a problem if prepare action is used in other tasks
but might be a problem in certain situations. Fixing the operators to be idempotent requires more complex
logic and support for Pig actions is missing currently.

Issue in Github: [FS Mapper and idempotence](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/175)

The dirFiles are not supported in FSMapper.

Issue in Github: [Add support for dirFiles in FsMapper](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/80)

## Java Example

The Java example can be run as:

`o2a -i examples/java -o output/java`

Make sure to first copy `examples/fs/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/java/` folder.

The converted DAG uses the `DataProcHadoopOperator` in Airflow.

### Known limitations

1. Overriding action's Main class via `oozie.launcher.action.main.class` is not implemented.

Issue in Github: [Override Java main class with property](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/338)

## Pig Example

The Pig example can be run as:

`o2a -i examples/pig -o output/pig`

Make sure to first copy `examples/pig/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/pig/` folder.

The converted DAG uses the `DataProcPigOperator` in Airflow.

### Known limitations
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

Issues in Github:
* [Add support for config-default.xml](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/137)
* [Add support for parameters section of the workflow.xml](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/138)
* [Handle global configuration properties](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/134)


## Shell Example

The Shell example can be run as:

`o2a -i examples/shell -o output/shell`

Make sure to first copy `examples/shell/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/shell/` folder.

The converted DAG uses the `BashOperator` in Airflow, which executes the desired shell
action with Pig by invoking `gcloud dataproc jobs submit pig --cluster=<cluster> --region=<region>
--execute 'sh <action> <args>'`.

### Known limitations

**1. Exit status not available**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/DG_ShellActionExtension.html):
> The output (STDOUT) of the Shell job can be made available to the workflow job after the Shell job ends.
This information could be used from within decision nodes.

Currently we use the `BashOperator` which can store only the last line of the job output in an XCOM.
In this case the line is not helpful as it relates to the Dataproc job submission status and
not the Shell action's result.

Issue in Github: [Finalize shell mapper](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/50)

**2. No Shell launcher configuration**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/DG_ShellActionExtension.html):
> Shell launcher configuration can be specified with a file, using the job-xml element, and inline,
using the configuration elements.

Currently there is no way specify the shell launcher configuration (it is ignored).

Issue in Github: [Shell Launcher Configuration](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/340)

## Spark Example

The Spark example can be run as:

`o2a -i examples/spark -o output/spark`

Make sure to first copy `/examples/spark/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/spark/` folder.

The converted DAG uses the `DataProcSparkOperator` in Airflow.

### Known limitations

**1. Only tasks written in Java are supported**

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
In this example the output (together with sub-worfklow dag) will be created in the `./output/subwf/` folder.

The converted DAG uses the `SubDagOperator` in Airflow.

### Known limitations

No known limitations.

## DistCp Example

The DistCp example can be run as:

`o2a -i examples/distcp -o output/distcp`

Make sure to first copy `examples/distcp/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/distcp/` folder.

The converted DAG uses the `BashOperator` in Airflow, which submits the Hadoop DistCp job using the
`gcloud dataproc jobs submit hadoop` command.

### Known limitations

The system test of the example run with Oozie fails due to unknown reasons. The converted DAG run by Airflow
completes successfully.

## Decision Example

The decision example can be run as:

`o2a -i examples/decision -o output/decision`

Make sure to first copy `examples/decision/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/decision/` folder.

The converted DAG uses the `BranchPythonOperator` in Airflow.

### Known limitations

Decision example is not yet fully functional as EL functions are not yet fully implemented so condition is
hard-coded for now. Once EL functions are implemented, the condition in the example will be updated.

Github issue: [Implement decision node](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/42)


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
In this example the output will be created in the `./output/el/` folder.

### Known limitations

Decision example is not yet fully functional as EL functions are not yet fully implemented so condition is
hard-coded for now. Once EL functions are implemented, the condition in the example will be updated.

Github issue: [Implement decision node](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/42)

## Hive/Hive2 Example

The Hive example can be run as:

`o2a -i examples/hive -o output/hive`

Make sure to first copy `/examples/hive/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

### Output
In this example the output will be created in the `./output/hive/` folder.

The converted DAG uses the `DataProcHiveOperator` in Airflow.

### Known limitations

**1. Only the connection to the local Hive instance is supported.**

Connection configuration options are not supported.


**2. Not all elements are supported**

For Hive, the following elements are not supported: `job-tracker`, `name-node`.
For Hive2, the following elements are not supported: `job-tracker`, `name-node`, `jdbc-url`, `password`.

The Github issue for both problems: [Hive connection configuration and other elements](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/342)

## Email Example

The Email example can be run as:

`o2a -i examples/email -o output/email`

Make sure to first copy `/examples/email/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.


### Output
In this example the output will be created in the `./output/email/` folder.

The converted DAG uses the `EmailOperator` in Airflow.

### Prerequisites
In Oozie the SMTP server configuration is located in `oozie-site.xml`.

For Airflow it needs to be located in `airflow.cfg`.
Example Airflow SMTP configuration:
```
[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
smtp_host = example.com
smtp_starttls = True
smtp_ssl = False
smtp_user = airflow_user
smtp_password = password
smtp_port = 587
smtp_mail_from = airflow_user@example.com
```

For more information on setting Airflow configuration options
[see here](https://airflow.readthedocs.io/en/stable/howto/set-config.html).

### Known limitations

**1. Attachments are not supported**

Due to the complexity of extracting files from HDFS inside Airflow and providing them
for the `EmailOperator`, the functionality of sending attachments has not yet been
implemented.

*Solution:* Implement in O2A a mechanism to extract a file from HDFS inside Airflow.

Github Issue: [Add support for attachment in Email mapper](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/335)

**2. `<content_type>` tag is not supported**

From Oozie docs:
> From uri:oozie:email-action:0.2 one can also specify mail content type as
<content_type>text/html</content_type>. “text/plain” is default.

Unfortunately, currently the `EmailOperator` only accepts the `mime_subtype` parameter.
However it only works for multipart subtypes, as the operator appends the subtype
to the `multipart/` prefix. Therefore passing either `html` or `plain` from Oozie makes no sense.

As a result the email will always be sent with the `EmailOperator`'s default Content-Type value,
which is `multipart/mixed`.

*Solution:* Modify the Airflow's `EmailOperator` to support more
content types.

Github Issue: [Content type support in Email mapper](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/343)

**3. `cc` and `bcc` fields are not templated in EmailOperator**

Only the 'to', 'subject' and 'html_content' fields in EmailOperator are templated.
In practice this covers all fields of an Oozie email action node apart from `cc` and `bcc`.

Therefore if there is an EL function in the action node in either of these two fields
which will require a Jinja expression in Airflow, it will not work - the expression will
not be executed, but rather treated as a plain string.

*Solution:* Modify the Airflow's `EmailOperator` to mark more fields as
`template_fields`.

Github Issue: [The CC: and BCC: fields are not templated in EmailOperator](https://github.com/GoogleCloudPlatform/oozie-to-airflow/issues/344)
