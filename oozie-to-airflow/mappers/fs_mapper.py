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
"""Maps FS node to Airflow's DAG"""

from typing import Set, Dict, List, Tuple
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task
from mappers.action_mapper import ActionMapper
from utils.relation_utils import chain
from utils.template_utils import render_template
from utils.el_utils import normalize_path_by_adding_hdfs_if_needed

ACTION_TYPE = "fs"

FS_OP_MKDIR = "mkdir"
FS_OP_DELETE = "delete"
FS_OP_MOVE = "move"
FS_OP_CHMOD = "chmod"
FS_OP_TOUCHZ = "touchz"
FS_OP_CHGRP = "chgrp"

FS_TAG_PATH = "path"
FS_TAG_SOURCE = "source"
FS_TAG_TARGET = "target"
FS_TAG_RECURSIVE = "recursive"
FS_TAG_DIRFILES = "dir-files"
FS_TAG_PERMISSIONS = "permissions"
FS_TAG_GROUP = "group"


def prepare_mkdir_command(node: Element, properties: Dict[str, str]) -> Tuple[str, List[str]]:
    path = normalize_path_by_adding_hdfs_if_needed(node.attrib[FS_TAG_PATH], properties)
    command = "fs -mkdir -p \\'{}\\'"
    return command, [path]


def prepare_delete_command(node: Element, properties: Dict[str, str]) -> Tuple[str, List[str]]:
    path = normalize_path_by_adding_hdfs_if_needed(node.attrib[FS_TAG_PATH], properties)
    command = "fs -rm -r \\'{}\\'"

    return command, [path]


def prepare_move_command(node: Element, properties: Dict[str, str]) -> Tuple[str, List[str]]:
    source = normalize_path_by_adding_hdfs_if_needed(node.attrib[FS_TAG_SOURCE], properties)
    target = normalize_path_by_adding_hdfs_if_needed(
        node.attrib[FS_TAG_TARGET], properties, allow_no_schema=True
    )

    command = "fs -mv \\'{}\\' \\'{}\\'"
    return command, [source, target]


def prepare_chmod_command(node: Element, properties: Dict[str, str]) -> Tuple[str, List[str]]:
    path = normalize_path_by_adding_hdfs_if_needed(node.attrib[FS_TAG_PATH], properties)
    permission = node.attrib[FS_TAG_PERMISSIONS]
    # TODO: Add support for dirFiles Reference: GH issues #80
    # dirFiles = bool_value(node, FS_TAG _DIRFILES)
    recursive = node.find(FS_TAG_RECURSIVE) is not None
    extra_param = "-R" if recursive else ""

    command = "fs -chmod {} \\'{}\\' \\'{}\\'"
    return command, [extra_param, permission, path]


def prepare_touchz_command(node: Element, properties: Dict[str, str]) -> Tuple[str, List[str]]:
    path = normalize_path_by_adding_hdfs_if_needed(node.attrib[FS_TAG_PATH], properties)

    command = "fs -touchz \\'{}\\'"
    return command, [path]


def prepare_chgrp_command(node: Element, properties: Dict[str, str]) -> Tuple[str, List[str]]:
    path = normalize_path_by_adding_hdfs_if_needed(node.attrib[FS_TAG_PATH], properties)
    group = node.attrib[FS_TAG_GROUP]

    recursive = node.find(FS_TAG_RECURSIVE) is not None
    extra_param = "-R" if recursive else ""

    command = "fs -chgrp {} \\'{}\\' \\'{}\\'"
    return command, [extra_param, group, path]


FS_OPERATION_MAPPERS = {
    FS_OP_MKDIR: prepare_mkdir_command,
    FS_OP_DELETE: prepare_delete_command,
    FS_OP_MOVE: prepare_move_command,
    FS_OP_CHMOD: prepare_chmod_command,
    FS_OP_TOUCHZ: prepare_touchz_command,
    FS_OP_CHGRP: prepare_chgrp_command,
}


class FsMapper(ActionMapper):
    """
    Converts a FS Oozie node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        properties: Dict[str, str],
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        super().__init__(
            oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, properties=properties, **kwargs
        )
        self.tasks: List[Task] = []

    def on_parse_node(self):
        super().on_parse_node()
        self.tasks = self.parse_tasks()

    def parse_tasks(self):
        if not list(self.oozie_node):
            return [Task(task_id=self.name, template_name="dummy.tpl", trigger_rule=self.trigger_rule)]

        return [self.parse_fs_action(i, node) for i, node in enumerate(self.oozie_node)]

    def convert_to_text(self):
        return render_template(template_name="action.tpl", tasks=self.tasks, relations=chain(self.tasks))

    def required_imports(self) -> Set[str]:
        return {"from airflow.operators import dummy_operator", "from airflow.operators import bash_operator"}

    @property
    def first_task_id(self):
        return self.tasks[0].task_id

    @property
    def last_task_id(self):
        return self.tasks[-1].task_id

    def parse_fs_action(self, index: int, node: Element):
        tag_name = node.tag
        tasks_count = len(self.oozie_node)
        task_id = self.name if tasks_count == 1 else f"{self.name}-fs-{index}-{tag_name}"
        mapper_fn = FS_OPERATION_MAPPERS.get(tag_name)

        if not mapper_fn:
            raise Exception("Unknown FS operation: {}".format(tag_name))

        pig_command, arguments = mapper_fn(node, self.properties)

        return Task(
            task_id=task_id,
            template_name="fs_op.tpl",
            template_params=dict(pig_command=pig_command, arguments=arguments),
        )
