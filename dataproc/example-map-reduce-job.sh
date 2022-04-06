#!/usr/bin/env bash
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
fixme
set -euxo pipefail

# ensure hdfs path exists
hdfs dfs -mkdir -p "/user/$(whoami)/"

# copy examples to home dir
cp /usr/local/lib/oozie/oozie-examples.tar.gz .

#unpack
tar -zxvf oozie-examples.tar.gz

# replace `localhost` with actual hostname
sed -i "s/localhost/$(hostname)/g" examples/apps/map-reduce/job.properties

# create directory on HDFS for current user
hdfs dfs -put examples "/user/$(whoami)/"

# run the MR task
oozie job -config examples/apps/map-reduce/job.properties -run
