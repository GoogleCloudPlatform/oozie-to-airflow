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
"""Maps Shell action into Airflow's DAG"""
import shlex
from typing import Dict, Set, Optional

import xml.etree.ElementTree as ET
from urllib.parse import urlparse

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin

from o2a.utils.xml_utils import get_tag_el_text

TAG_GIT_URI = "git-uri"
TAG_BRANCH = "branch"
TAG_KEY_PATH = "key-path"
TAG_DESTINATION_URI = "destination-uri"


def prepare_git_command(
    git_uri: str, git_branch: Optional[str], destination_path: str, key_path: Optional[str]
):
    cmd = (
        f"$DAGS_FOLDER/../data/git.sh "
        "--cluster {dataproc_cluster} "
        "--region {gcp_region} "
        f"--git-uri {shlex.quote(git_uri)} "
        f"--destination-path {shlex.quote(destination_path)}"
    )

    if git_branch:
        cmd += f" --branch {shlex.quote(git_branch)}"

    if key_path:
        cmd += f" --key-path {shlex.quote(key_path)}"

    return cmd


class GitMapper(ActionMapper, PrepareMixin):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    bash_command: str

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, name, trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.params = params
        self.trigger_rule = trigger_rule

    def on_parse_node(self):
        git_uri = get_tag_el_text(self.oozie_node, TAG_GIT_URI, self.params)
        git_branch = get_tag_el_text(self.oozie_node, TAG_BRANCH, self.params)
        destination_uri = get_tag_el_text(self.oozie_node, TAG_DESTINATION_URI, self.params)
        destination_path = urlparse(destination_uri).path
        key_path_uri = get_tag_el_text(self.oozie_node, TAG_KEY_PATH, self.params)
        key_path = urlparse(key_path_uri).path
        self.bash_command = prepare_git_command(
            git_uri=git_uri, git_branch=git_branch, destination_path=destination_path, key_path=key_path
        )

    def to_tasks_and_relations(self):
        tasks = [
            Task(
                task_id=self.name,
                template_name="git.tpl",
                template_params=dict(bash_command=self.bash_command),
            )
        ]
        relations = []
        if self.has_prepare(self.oozie_node):
            prepare_command = self.get_prepare_command(self.oozie_node, self.params)
            tasks.insert(
                0,
                Task(
                    task_id=self.name + "_prepare",
                    template_name="prepare.tpl",
                    template_params=dict(prepare_command=prepare_command),
                ),
            )
            relations = [Relation(from_task_id=self.name + "_prepare", to_task_id=self.name)]
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self):
        return "{task_id}_prepare".format(task_id=self.name)
