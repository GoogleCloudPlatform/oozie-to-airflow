# -*- coding: utf-8 -*-
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
"""Mappers defined for the converter.

This module contains mappings between Oozie actions and corresponding mappers that handle
particular actions.

"""

from typing import Type, Dict

from o2a.mappers.distcp_mapper import DistCpMapper
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.java_mapper import JavaMapper
from o2a.mappers.email_mapper import EmailMapper
from o2a.mappers.fs_mapper import FsMapper
from o2a.mappers.git_mapper import GitMapper
from o2a.mappers.hive_mapper import HiveMapper
from o2a.mappers.mapreduce_mapper import MapReduceMapper
from o2a.mappers.pig_mapper import PigMapper
from o2a.mappers.shell_mapper import ShellMapper
from o2a.mappers.spark_mapper import SparkMapper
from o2a.mappers.ssh_mapper import SSHMapper
from o2a.mappers.subworkflow_mapper import SubworkflowMapper

ACTION_MAP: Dict[str, Type[ActionMapper]] = {
    "ssh": SSHMapper,
    "spark": SparkMapper,
    "pig": PigMapper,
    "fs": FsMapper,
    "java": JavaMapper,
    "sub-workflow": SubworkflowMapper,
    "shell": ShellMapper,
    "map-reduce": MapReduceMapper,
    "git": GitMapper,
    "hive": HiveMapper,
    "hive2": HiveMapper,
    "distcp": DistCpMapper,
    "email": EmailMapper,
}
