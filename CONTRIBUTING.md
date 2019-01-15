<!--
Copyright 2018 Google LLC

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

# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Development Workflow Setup (Composer/Dataproc)

An easy way of testing changes to the oozie-to-airflow converter is by using  Cloud Composer and Dataproc. These services allow testing without much need for an on-prem setup.

#### Setup
1. Create a [Dataproc cluster with Oozie initialization actions.](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/oozie#oozie)
1. Run the `setup.py` script to install required python dependencies on the dev machine
1. Install other required depedencies, for example, Airflow's `SparkSubmitOperator` requires `spark-submit` installed on the cluster.
1. [Create a Cloud Composer Environment](https://cloud.google.com/composer/docs/how-to/managing/creating#creating_a_new_environment) with at least Airflow version 1.10 to test the Apache Airflow workflows. (Since Airflow 1.10 is in  Beta for Cloud Composer, you must [enable beta features in Cloud Console](https://cloud.google.com/composer/docs/concepts/beta-support#enable-beta))
1. Set up all required [Airflow Connections](https://airflow.apache.org/howto/manage-connections.html). This is required for things like `SSHOperator`, and `SparkSubmitOperator`.
1. _(Optional)_ Use [gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse) to link a local directory on the Dataproc cluster to the DAG folder in the [GCS bucket that Composer creates](https://cloud.google.com/composer/docs/concepts/cloud-storage).

## Development Workflow Setup (Local)

As opposed to being able to test on Google Cloud you can easily contribute to the project using just your local machine. The setup is very similar to the cloud setup.

#### Setup
1. Install [Apache Airflow](https://airflow.apache.org/start.html) and [Apache Oozie](https://oozie.apache.org/docs/4.3.1/DG_QuickStart.html) locally
1. Run the `setup.py` script to install the required python dependencies
1. Install other required depedencies, for example, Airflow's `SparkSubmitOperator` requires `spark-submit` installed on the cluster.
1. Set up all required [Airflow Connections](https://airflow.apache.org/howto/manage-connections.html). This is required for things like `SSHOperator`, and `SparkSubmitOperator`.
1. _(Optional)_ Use [gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse) to link a local directory to the DAG folder in the [GCS bucket that Composer creates](https://cloud.google.com/composer/docs/concepts/cloud-storage).

## Testing Changes
1. Make desired changes to the `oozie-to-airflow` application
1. Run all unit tests to check for regressions (be sure to new write tests!)
    1. `python3 -m unittest discover /path/to/tests/`
1. Run desired Oozie XML on Dataproc's Oozie instance to make sure it is valid and can be executed successfully
1. Use the converter on any applicable examples
1. Move the output python file to the DAG folder in the GCS Bucket
1. Check to see if working as expected on Cloud Composer
1. Open a pull request with your change on our [GitHub](https://github.com/GoogleCloudPlatform/google-cloud).

## Adding Action Nodes

Please keep in mind, we are targeting Apache Airflow 1.10 and Oozie 1.0.

In order to add an action node there are a few steps that need to be taken:

1. Create a python class in the `mappers` module that extends the `ActionMapper`
class.
    1. Override the method `convert_to_text`. In this method, return a string that
     looks like the corresponding DAG, this method will be written to file.
    1. Override the method `convert_to_airflow_op`. This will return an actual
     python object that is an Airflow operator. This isn't currently used, but
     it might be helpful in the future, so it is not necessarily required (as of
     now).
    1. Lastly, override the method `required_imports`, this method returns a list
     of strings, where each string is an `import` statement that the mapped
     airflow operator requires to run. For example, it might `return ['from
     airflow.contrib.operators import ssh_operator']`

1. Next, edit `oozie_parser.py` and add the name of the oozie action node as the key
of the `ACTION_MAP` dictionary with the value being the python class it will
map to.

1. Lastly, add tests to the tests/mappers directory with `test_<MAPPER_NAME>.py`
   as the name.
