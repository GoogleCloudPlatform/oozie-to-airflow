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
(DAGs) of tasks in python. The Airflow scheduler executes your tasks on an array of
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
* python > 3.6
* see [requirements.txt](requirements.txt)

Additionally the shell script included in the directory, `init.sh`, can
be ran to set up the dependencies and ready your machine to run the examples.

```bash
# Allow init.sh to execute
$ chmod +x init.sh
# Execute init.sh
$ ./init.sh
```

You can run the program (minimally) by calling:
`python o2a.py -i <INPUT_WORKFLOW_XML>`

You can also specify the job.properties file, user, and output file.

`python o2a.py -i <INPUT_FILE> -p <PROP_FILE> -u <USER> -o
<OUTPUT_FILE>`

#### Known Limitations

The goal of this program is to mimic both the actions and control flow
that is outlined by the Oozie workflow file. Unfortunately there are some
limitations as of now that have not been worked around regarding the execution
flow. The situation where the execution path might not execute correctly is when
there are 4 nodes, A, B, C, D, with the following Oozie specified execution paths
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

#### EL Functions

As of now, a very minimal set of [Oozie EL](https://oozie.apache.org/docs/4.0.1/WorkflowFunctionalSpec.html#a4.2_Expression_Language_Functions)
functions are supported. The way they work is that there exists a
dictionary mapping from each Oozie EL function string to the
corresponding python function. This is in `utils/el_utils.py`.
This design allows for custom EL function mapping if one so chooses. By
default everything gets mapped to the module `o2a_libs`. This means in
order to use EL function mapping, the folder `o2a_libs` should
be copied over to the Airflow DAG folder. This should then be picked up and
parsed by the Airflow workers and then available to all DAGs.

#### Command Line Flags

| Flag                                  | Meaning                                                                                      |
|---------------------------------------|----------------------------------------------------------------------------------------------|
| -h/--help                             | Shows help message and exits                                                                 |
| -i INPUT/--input INPUT                | Path to the XML file to be converted                                                         |
| -o OUTPUT/--output OUTPUT             | Desired output python file name (optional)                                                              |
| -d DAG/--dag DAG                      | Desired Airflow DAG name (optional)                                                                     |
| -p PROPERTIES/--properties PROPERTIES | Path to the job.properties file (optional)                                                   |
| -u USER/--user USER                   | The user to be replaced for ${user.name}. If none specified, current user is used (optional) |

## Examples

All examples can be found in the `examples/` directory.

### Demo Example

The demo example contains several action and control nodes. The control
nodes are fork, join, decision, start, end, and kill. As far as action
nodes go, there are fs, map-reduce, and pig. Unfortunately, none of these
are currently supported, but when the program encounters a node it does
not know how to parse, it will perform a sort of "skeleton transformation"
where it will convert all the unknown nodes to dummy nodes, which will
allow users to manually parse the nodes if they so wish as the control flow
is there.

The demo can be run as:

`python o2a.py -i examples/demo/workflow.xml -p examples/demo/job.properties`

This will parse and write to an output file (since no -o flag). The output
file will be noted in the logs.

Note: The decision node is not fully functional as there is not currently
support for all EL functions. So in order for it to run in Airflow you must
edit the python output file and change the decision node expression.


### SSH Example

The ssh example can be run as:

`python o2a.py -i examples/ssh/workflow.xml -p examples/ssh/job.properties -o output.py`

This will convert the specified Oozie XML and write the output into the
specified output file, in this case `output.py`. There are some differences
between Apache Oozie and Apache Airflow as far as the SSH specification goes.
In Airflow you will have to add/edit an SSH specific Connection that contains
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

More information can be found on [Airflow's Website](https://airflow.apache.org/cli.html#connections)

### MapReduce Example

The MapReduce example can be run as:

`python o2a.py -i examples/mapreduce -o output/mapreduce`

Make sure to first copy `/examples/mapreduce/configuration-template.properties`, rename it as
`configuration.properties` and fill in with configuration data.

The output will appear by default in `/output/mapreduce/mapreduce.py`.

#### Current limitations

**1. Oozie workflow XSD version 0.5**

For now the example uses older version of XSD and not the newest 1.0 version. That's because the
system tests are run on a Dataproc cluster which installs Oozie version
4.3.0 using an init-action, and this version only supports schema version 0.5.

There are a few differences between 0.5 and 1.0, such as `<job-tracker>` in 0.5 vs
`<resource-manager>` in 1.0.
We will try to upgrade the Oozie version in Dataproc to 5.1.0 and subsequently update
the example code.

**2. Exit status not available**

From the [Oozie documentation](https://oozie.apache.org/docs/5.1.0/WorkflowFunctionalSpec.html#a3.2.2_Map-Reduce_Action):
> The counters of the Hadoop job and job exit status (FAILED, KILLED or SUCCEEDED) must be available to the
workflow job after the Hadoop jobs ends. This information can be used from within decision nodes and other
actions configurations.

Currently we use the `DataProcHadoopOperator` which does not store the job exit status in an XCOM for other tasks to use.

**3. Configuration options**

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

**4. Streaming and pipes**

Streaming and pipes are currently not supported.

### Spark Example

The spark example, like the other examples can be run as:

`python o2a.py -i examples/spark/workflow.xml -p examples/spark/job.properties -o output.py`

This will write the XML file to `output.py`. The spark node, similarly
to the SSH node requires editing of an Airflow Connection to specify the
spark cluster is running. This can be found under **Admin >> Connections**
or created from the command line (see above).

### EL Example

The Oozie Expression Language (EL) example can be run as:
`python o2a.py -i examples/ssh/workflow.xml -p examples/el/job.properties -o output.py`

This will showcase the ability to use the `o2a_libs` directory to map EL functions
to python methods. This example assumes that the user has a valid Apache Airflow
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
* Init action: TODO


### Running the tests

We can run examples defined in examples folder to run system tests. The system tests use a real
Dataproc Cluster, Composer and Oozie run in Dataproc cluster (in the future) to run the tests automatically.

Each test consists of several phases:

* convert - converts workflow application of Oozie to an Airflow DAG
* prepare-dataproc - prepares Dataproc by creating all necessary Hadoop file structure
* prepare-composer - prepares Composer by copying reusable libraries and scripts to Airflow's bucket
* test-composer - runs tests: converted files are copied to Composer bucket and DAG is triggered

By default all phases of test are executed but you can choose to execute only one by using `-p` flag.

You can run the tests using this command:

`./run-sys-tests -a <APPLICATION> `


When you run it with `--help` you can see all the options. You can setup autocomplete
with `-A` option - this way you do not have to remember all the options.

Current options:

```bash
Usage: run-sys-test [FLAGS]

Executes prepare or run phase for integration testing of O2A converter.

Flags:

-h, --help
        Shows this help message.

-p, --phase <PHASE>
        Phase of the test to run. One of [ convert prepare-dataproc prepare-composer test-composer all ]. Defaults to all.

-a, --application <APPLICATION>
        Application (from examples dir) to run the tests on. Defaults to

-C, --composer-name <COMPOSER_NAME>
        Composer instance used to run the operations on. Defaults to o2a-integration

-L, --composer-location <COMPOSER_LOCATION>
        Composer locations. Defaults to europe-west1

-c, --cluster <CLUSTER>
        Cluster used to run the operations on. Defaults to oozie-o2a-2cpu

-b, --bucket <BUCKET>
        Airflow Composer DAG bucket used. Defaults to europe-west1-o2a-integratio-f690ede2-bucket

-m, --master <MASTER>
        Cluster master used to run most operations on. Defaults to <CLUSTER_NAME>-m

-r, --region <REGION>
        GCP Region where the cluster is located. Defaults to europe-west3

-z, --zone <ZONE>
        GCP Zone where the master is located. Defaults to europe-west3-c

-A, --setup-autocomplete
        Sets up autocomplete for run-sys-tests
```

# Running all example conversions

All example conversions can by run via the `./run-all-conversions` script. It is also executed during automated tests.
