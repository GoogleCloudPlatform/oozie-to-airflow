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

# Cloud Composer Tools and Examples

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/cloud-composer.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/cloud-composer)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/cloud-composer/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/cloud-composer)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Updates](https://pyup.io/repos/github/GoogleCloudPlatform/cloud-composer/shield.svg)](https://pyup.io/repos/github/GoogleCloudPlatform/cloud-composer/)
[![Python 3](https://pyup.io/repos/github/GoogleCloudPlatform/cloud-composer/python-3-shield.svg)](https://pyup.io/repos/github/GoogleCloudPlatform/cloud-composer/)

Python3.6 is required to run the project.

We are using a number of checks for quality checks of the code. They are verified during Travis build but
also you can install pre-commit hook by running:

`pre-commit install`


You can run all the checks manually by running:

`pre-commit run --all-files`

You might need to instal xmllint if you do not have it locally. This can be done
with `apt install libxml2-utils` on Linux or `brew install xmlstarlet` on MacOS.

You can always skip running the tests by providing `--no-verify` flag to `git commit` command.

You can check all commands of pre-commit framework at https://pre-commit.com/


# Running Oozie 5.1.0 in Dataproc

We prepared Dataproc [initialization action](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions)
that allows to run Oozie 5.1.0 on Dataproc.

Please upload `dataproc/oozie-5.1.sh` to your GCS bucket and create cluster using following command:
```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
  --single-node \
  --image-version 1.3-debian9 \
  --initialization-actions <PATH_TO_INIT_ACTION_ON_YOUR_GCS> \
  --initialization-action-timeout=30m
```
**note 1:** it might take ~20 minutes to create the cluster

**note 2:** the init-action works only with [single-node cluster](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/single-node-clusters)
and Dataproc 1.3

Once cluster is created, steps from `dataproc/example-map-reduce.job.sh` can be run on master node to execute
Oozie's example Map-Reduce job.

Oozie is serving web UI on port 11000. To enable access to it please follow [official instructions](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces)
on how to connect to the cluster web interfaces.

List of jobs with their statuses can be also shown by issuing `oozie jobs` command on master node.
