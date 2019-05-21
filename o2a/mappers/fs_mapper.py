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

import shlex
from typing import Set, List
from xml.etree.ElementTree import Element

from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.utils.relation_utils import chain
from o2a.utils.el_utils import normalize_path

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


def prepare_mkdir_command(node: Element, params):
    path = normalize_path(node.attrib[FS_TAG_PATH], params)
    command = "fs -mkdir -p {path}".format(path=shlex.quote(path))
    return command


def prepare_delete_command(node: Element, params):
    path = normalize_path(node.attrib[FS_TAG_PATH], params)
    command = "fs -rm -r {path}".format(path=shlex.quote(path))

    return command


def prepare_move_command(node: Element, params):
    source = normalize_path(node.attrib[FS_TAG_SOURCE], params)
    target = normalize_path(node.attrib[FS_TAG_TARGET], params, allow_no_schema=True)

    command = "fs -mv {source} {target}".format(source=shlex.quote(source), target=shlex.quote(target))
    return command


def prepare_chmod_command(node: Element, params):
    path = normalize_path(node.attrib[FS_TAG_PATH], params)
    permission = node.attrib[FS_TAG_PERMISSIONS]
    # TODO: Add support for dirFiles Reference: GH issues #80
    # dirFiles = bool_value(node, FS_TAG _DIRFILES)
    recursive = node.find(FS_TAG_RECURSIVE) is not None
    extra_param = "-R" if recursive else ""

    command = "fs -chmod {extra} {permission} {path}".format(
        extra=extra_param, path=shlex.quote(path), permission=shlex.quote(permission)
    )
    return command


def prepare_touchz_command(node: Element, params):
    path = normalize_path(node.attrib[FS_TAG_PATH], params)

    command = "fs -touchz {path}".format(path=shlex.quote(path))
    return command


def prepare_chgrp_command(node: Element, params):
    path = normalize_path(node.attrib[FS_TAG_PATH], params)
    group = node.attrib[FS_TAG_GROUP]

    recursive = node.find(FS_TAG_RECURSIVE) is not None
    extra_param = "-R" if recursive else ""

    command = "fs -chgrp {extra} {group} {path}".format(
        extra=extra_param, group=shlex.quote(group), path=shlex.quote(path)
    )
    return command


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

    tasks: List[Task]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = []

    def on_parse_node(self):
        super().on_parse_node()
        self.tasks = self.parse_tasks()

    def parse_tasks(self):
        if not list(self.oozie_node):
            return [Task(task_id=self.name, template_name="dummy.tpl", trigger_rule=self.trigger_rule)]

        return [self.parse_fs_action(i, node) for i, node in enumerate(self.oozie_node)]

    def to_tasks_and_relations(self):
        return self.tasks, chain(self.tasks)

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.operators import dummy_operator",
            "from airflow.operators import bash_operator",
            "import shlex",
        }

    @property
    def first_task_id(self):
        return self.tasks[0].task_id

    @property
    def last_task_id(self):
        return self.tasks[-1].task_id

    def parse_fs_action(self, index: int, node: Element):
        tag_name = node.tag
        tasks_count = len(self.oozie_node)
        task_id = self.name if tasks_count == 1 else f"{self.name}_fs_{index}_{tag_name}"
        mapper_fn = FS_OPERATION_MAPPERS.get(tag_name)

        if not mapper_fn:
            raise Exception("Unknown FS operation: {}".format(tag_name))

        pig_command = mapper_fn(node, self.params)

        return Task(task_id=task_id, template_name="fs_op.tpl", template_params=dict(pig_command=pig_command))
