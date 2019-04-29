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

set -euxo pipefail


function retry_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_maven() {
  retry_command "apt-get update"
  retry_command "apt-get install -y maven"
}

function get_oozie(){
  wget https://www.apache.org/dist/oozie/5.1.0/oozie-5.1.0.tar.gz -P /tmp
  tar -xzvf /tmp/oozie-5.1.0.tar.gz --directory /tmp/
}

function build_oozie(){
  /tmp/oozie-5.1.0/bin/mkdistro.sh -DskipTests -Puber
  tar -zxvf /tmp/oozie-5.1.0/distro/target/oozie-5.1.0-distro.tar.gz -C /usr/local/lib
  mv /usr/local/lib/oozie-5.1.0/ /usr/local/lib/oozie
}

function configure_oozie() {
  adduser --disabled-password --gecos ""  oozie

  mkdir /usr/local/lib/oozie/libext

  # download ext-2.2 for web UI
  wget http://archive.cloudera.com/gplextras/misc/ext-2.2.zip -P /usr/local/lib/oozie/libext

  rm /usr/local/lib/oozie/conf/hadoop-conf/*
  ln -s /etc/hadoop/conf/*-site.xml /usr/local/lib/oozie/conf/hadoop-conf/
  tar -zxvf /usr/local/lib/oozie/oozie-sharelib-5.1.0.tar.gz -C /usr/local/lib/oozie/

  chown oozie:oozie -R /usr/local/lib/oozie
}

function configure_hadoop() {
  # Remove jackson jars specific for pig so that the main hadoop ones are used instead
  sudo rm -f /usr/local/lib/oozie/share/lib/pig/jackson*

  hdfs dfs -mkdir -p /user/oozie/
  hadoop fs -put -f /usr/local/lib/oozie/share /user/oozie/

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.hosts' --value '*' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.groups' --value '*' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/mapred-site.xml" \
    --name 'yarn.app.mapreduce.am.resource.mb' --value '1024' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/mapred-site.xml" \
    --name 'mapreduce.map.java.opts' --value '-Xmx777m' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/mapred-site.xml" \
    --name 'mapreduce.reduce.java.opts' --value '-Xmx777m' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/mapred-site.xml" \
    --name 'mapreduce.map.memory.mb' --value '1024' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/mapred-site.xml" \
    --name 'mapreduce.reduce.memory.mb' --value '1024' \
    --clobber


  # restart hadoop services
  for service in hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-yarn-resourcemanager; do
    if [[ $(systemctl list-unit-files | grep ${service}) != '' ]] && \
      [[ $(systemctl is-enabled ${service}) == 'enabled' ]]; then
      systemctl restart ${service}
    fi
  done

}

function create_executables() {
  cat << EOF > /usr/bin/oozie
#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Autodetect JAVA_HOME if not defined

. /usr/lib/bigtop-utils/bigtop-detect-javahome
exec /usr/local/lib/oozie/bin/oozie "\$@"
EOF

  cat << EOF > /usr/bin/oozied
#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Autodetect JAVA_HOME if not defined

. /usr/lib/bigtop-utils/bigtop-detect-javahome
exec /usr/local/lib/oozie/bin/oozied.sh "\$@"
EOF
  chmod 755 /usr/bin/oozie
  chmod 755 /usr/bin/oozied
}

function start_oozie() {
  sudo -H -u oozie bash -c '/usr/bin/oozied start'
}

function main() {
    install_maven
    get_oozie
    build_oozie
    configure_oozie
    configure_hadoop
    create_executables
    start_oozie
}

main
