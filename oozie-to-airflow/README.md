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

## Oozie to Airflow

An effort by the Cloud Composer team to create tool to easily convert between
[Apache Oozie](http://oozie.apache.org/) workflows and [Apache Airflow](https://airflow.apache.org)
workflows.

The program targets Apache Airflow >= 1.10 and Apache Oozie 1.0 XML schema.

### Background
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

### Oozie Control Nodes
#### Fork

A [fork node](https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.5_Fork_and_Join_Control_Nodes)
splits the path of execution into multiple concurrent paths of execution.

#### Join

A [join node](https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.5_Fork_and_Join_Control_Nodes)
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
#### Decision

A [decision node](https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.4_Decision_Control_Node)
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
#### Start

The [start node](https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.1_Start_Control_Node)
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
#### End

The [end node](https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.2_End_Control_Node)
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

#### Kill

The [kill node](https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.3_Kill_Control_Node)
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

## Running the Program

#### Required Python Dependencies
* Python > 3.6
* See [requirements.txt](../requirements.txt)

Additionally the shell script included in the directory, `init.sh`, can
be ran to set up the dependencies and ready your machine to run the examples.

```bash
# Allow init.sh to execute
$ chmod +x init.sh
# Execute init.sh
$ ./init.sh
```

You can run the program (minimally) by calling:
`python o2a.py -i <INPUT_WORKFLOW_FOLDER_PATH> -o <OUTPUT_FOLDER_PATH>`

Example:
`python o2a.py -i examples/demo -o output/demo`

This is the full usage guide, available by running `python o2a.py -h`

```
usage: o2a.py [-h] -i INPUT_DIRECTORY_PATH -o OUTPUT_DIRECTORY_PATH
              [-d DAG_NAME] [-u USER] [-s START_DAYS_AGO]
              [-v SCHEDULE_INTERVAL]

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

#### Known Limitations

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


#### EL Functions

As of now, a very minimal set of [Oozie EL](https://oozie.apache.org/docs/4.0.1/WorkflowFunctionalSpec.html#a4.2_Expression_Language_Functions)
functions are supported. The way they work is that there exists a
dictionary mapping from each Oozie EL function string to the
corresponding Python function. This is in `utils/el_utils.py`.
This design allows for custom EL function mapping if one so chooses. By
default everything gets mapped to the module `o2a_libs`. This means in
order to use EL function mapping, the folder `o2a_libs` should
be copied over to the Airflow DAG folder. This should then be picked up and
parsed by the Airflow workers and then available to all DAGs.

## Examples

All examples can be found in the `examples/` directory.

* [Demo](#demo-example)
* [SSH](#ssh-example)
* [MapReduce](#mapreduce-example)
* [Pig](#pig-example)
* [Shell](#shell-example)
* [Sub-workflow](#sub-workflow-example)
* [Decision](#decision-example)
* [EL](#el-example)

### Demo Example

The demo example contains several action and control nodes. The control
nodes are `fork`, `join`, `decision`, `start`, `end`, and `kill`. As far as action
nodes go, there are `fs`, `map-reduce`, and `pig`.

Most of these are already supported, but when the program encounters a node it does
not know how to parse, it will perform a sort of "skeleton transformation" -
it will convert all the unknown nodes to dummy nodes. This will
allow users to manually parse the nodes if they so wish as the control flow
is there.

The demo can be run as:

`python o2a.py -i examples/demo -o output/demo`

This will parse and write to an output file in the `output/demo` directory.

**Note:** The decision node is not fully functional as there is not currently
support for all EL functions. So in order for it to run in Airflow you must
edit the Python output file and change the decision node expression.


### SSH Example

The ssh example can be run as:

`python o2a.py -i examples/ssh -o output/ssh`

This will convert the specified Oozie XML and write the output into the
specified output directory, in this case `output/ssh/ssh.py`.

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

### MapReduce Example

The MapReduce example can be run as:

`python o2a.py -i examples/mapreduce -o output/mapreduce`

Make sure to first copy `/examples/mapreduce/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

##### Output
In this example the output will appear in `/output/mapreduce/mapreduce.py`.

The converted DAG uses the `DataProcHadoopOperator` in Airflow.

#### Current limitations

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


### Pig Example

The Pig example can be run as:

`python o2a.py -i examples/pig -o output/pig`

Make sure to first copy `/examples/pig/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

##### Output
In this example the output will appear in `/output/pig/pig.py`.

The converted DAG uses the `DataProcPigOperator` in Airflow.

#### Current limitations

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


### Shell Example

The Shell example can be run as:

`python o2a.py -i examples/shell -o output/shell`

Make sure to first copy `/examples/shell/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

##### Output
In this example the output will appear in `/output/shell/shell.py`.

The converted DAG uses the `BashOperator` in Airflow, which executes the desired shell
action with Pig by invoking `gcloud dataproc jobs submit pig --cluster=<cluster> --region=<region>
--execute 'sh <action> <args>'`.

#### Current limitations

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


### Sub-workflow Example

The Sub-workflow example can be run as:

`python o2a.py -i examples/subwf -o output/subwf`

Make sure to first copy `/examples/subwf/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

##### Output
In this example the output will appear in `/output/subwf/subwf.py`.
Additionally, a `subdag_test.py` (name to be changed soon) file is generated in the same directory,
which contains the factory method `sub_dag()` returning the actual Airflow subdag.

The converted DAG uses the `SubDagOperator` in Airflow.


### Decision Example

The decision example can be run as:

`python o2a.py -i examples/decision -o output/decision`

Make sure to first copy `/examples/decision/configuration.template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

##### Output
In this example the output will appear in `/output/decision/decision.py`.

The converted DAG uses the `BranchPythonOperator` in Airflow.


### EL Example

The Oozie Expression Language (EL) example can be run as:
`python o2a.py -i examples/el -o output/el`

This will showcase the ability to use the `o2a_libs` directory to map EL functions
to Python methods. This example assumes that the user has a valid Apache Airflow
SSH connection set up and the `o2a_libs` directory has been copied to the dags
folder.

Please keep in mind that as of the current version only a single EL variable
or single EL function. Variable/function chaining is not currently supported.

## Running Tests

Currently, the test directory is set up in a such a way that the folders in `tests/` directory mirrors the structure of the `oozie-to-airflow` directory. For example, if we have `oozie-to-airflow/o2a_libs/helper_functions.py` the tests for that file would be in `tests/o2a_libs/test_helper_functions.py`.

There are several ways to tests for the various different tests that you want to run.

To run all the tests in a given directory call the below command:

```
python -m unittest discover /path/to/tests/directory/
```

To run all the tests in a given file call the below command:
```
python -m unittest /path/to/test/file.py
```

## System Tests

Oozie to Airflow has a set of system tests that test end-2-end functionality of conversion and execution
of workflows using Cloud Dataproc and Cloud Composer.

### System test environment

Cloud Composer:
* composer-1.5.0-airflow-1.10.1
* python version 3 (3.6.6)
* machine n1-standard-1
* node count: 3
* Additional pypi packages:
    * sshtunnel==0.1.4

Cloud Dataproc Cluster with Oozie
* n1-standard-2, 2 vCPU, 7.50 GB memory
* primary disk size, 50 GB
* Image 1.3.29-debian9
* Hadoop version
* Init action: [oozie-5.1.sh](dataproc\oozie-5.1.sh)

### Example apps

This folder contains example applications that can be run to run both Composer and Oozie jobs for the apps

Structure of the application folder:

```
<APPLICATION>/
             |- workflow.xml    - Oozie workflow xml (1.0 schema)
             |- job.properties  - job properties that are used to run the job
             |- assets          - folder with extra assets that should be copied to application folder in HDFS
             |- configuration.template.properties - template of configuration values used during conversion
             |- configuration.properties - generated properties for configuration values
```

### Running the system tests

We can run examples defined in examples folder as system tests. The system tests use an existing
Composer, Dataproc cluster and Oozie run in the Dataproc cluster to prepare HDFS application folder structure
and trigger the tests automatically.

You can run the tests using this command:

`./run-sys-tests --application <APPLICATION> --phase <PHASE>`

Default phase is convert - it only converts the oozie workflow to Airflow DAG without running the tests
on either Oozie nor Composer

When you run the script with `--help` you can see all the options. You can setup autocomplete
with `-A` option - this way you do not have to remember all the options.

Current options:

```
Usage: run-sys-test [FLAGS] [-A|-S]

Executes prepare or run phase for integration testing of O2A converter.

Flags:

-h, --help
        Shows this help message.

-a, --application <APPLICATION>
        Application (from examples dir) to run the tests on. Must be specified unless -S or -A are specified.

-p, --phase <PHASE>
        Phase of the test to run. One of [ convert prepare-dataproc prepare-composer test-composer test-oozie ]. Defaults to convert.

-C, --composer-name <COMPOSER_NAME>
        Composer instance used to run the operations on. Defaults to o2a-integration

-L, --composer-location <COMPOSER_LOCATION>
        Composer locations. Defaults to europe-west1

-c, --cluster <CLUSTER>
        Cluster used to run the operations on. Defaults to oozie-51

-b, --bucket <BUCKET>
        Airflow Composer DAG bucket used. Defaults to bucket that is used by Composer.

-r, --region <REGION>
        GCP Region where the cluster is located. Defaults to europe-west3

-v, --verbose
        Add even more verbosity when running the script.


Optional commands to execute:


-S, --ssh-to-cluster-master
        SSH to dataproc's cluster master. Arguments after -- are passed to gcloud ssh command as extra args.

-A, --setup-autocomplete
        Sets up autocomplete for run-sys-tests
```

### Re-running the tests

You do not need to specify the parameters once you run the script with your chosen flags.
The latest parameters used are stored and cached locally in .ENVIRONMENT_NAME files and used next time
when you run the script:

    .COMPOSER_DAG_BUCKET
    .COMPOSER_LOCATION
    .COMPOSER_NAME
    .DATAPROC_CLUSTER_NAME
    .GCP_REGION
    .LOCAL_APP_NAME
    .PHASE


### Test phases

The following phases are defined for the system tests:

* prepare-configuration - prepares configuration based on passed Dataproc/Composer parameters

* convert - converts the example application workflow to DAG and stores it in output/<APPLICATION> directory

* prepare-dataproc - prepares Dataproc cluster to execute both Composer and Oozie jobs. The preparation is:

   * Local filesystem: `${HOME}/o2a/<APPLICATION>` directory contains application to be uploaded to HDFS

   * Local filesystem: `${HOME}/o2a/<APPLICATION>.properties` property file to run the oozie job

   * HDFS: /user/${user.name}/examples/apps/<APPLICATION> - the application is stored in this HDFS directory

* test-composer - runs tests on Composer instance

* test-oozie - runs tests on Oozie in Hadoop cluster

The typical scenario to run the tests are:

Running application via oozie:
```
./run-sys-test --phase prepare-dataproc --application <APP> --cluster <CLUSTER>

./run-sys-test --phase test-oozie
```

Running application via composer:
```
./run-sys-test --phase prepare-dataproc --application <APP> --cluster <CLUSTER>

./run-sys-test --phase test-composer
```

### Running sub-workflows

In order to run sub-workflows you need to have the sub-workflow application already present in HDFS,
therefore you need to run at least  `./run-sys-test --phase prepare-dataproc --application <SUBWORKFLOW_APP>`

For example in case of the demo application, you need to run at least once
`./run-sys-test --phase prepare-dataproc --application mapreduce` because mapreduce is used as sub-workflow
in the demo application.

### Running all example conversions

All example conversions can by run via the `./run-all-conversions` script. It is also executed during
automated tests.
