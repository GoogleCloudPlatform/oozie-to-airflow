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
from typing import List, Optional, Set, Tuple

from urllib.parse import urlparse
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.o2a_libs.property_utils import PropertySet

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
        "--cluster {{params.configuration_properties['dataproc_cluster']}} "
        "--region {{params.configuration_properties['gcp_region']}} "
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

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        property_set: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            trigger_rule=trigger_rule,
            property_set=property_set,
            **kwargs,
        )
        PrepareMixin.__init__(self, oozie_node=oozie_node)
        self.git_uri: Optional[str] = None
        self.git_branch: Optional[str] = None
        self.destination_uri: Optional[str] = None
        self.destination_path: Optional[str] = None
        self.key_path_uri: Optional[str] = None
        self.key_path: Optional[str] = None

    def on_parse_node(self):
        super().on_parse_node()
        self.git_uri = get_tag_el_text(
            self.oozie_node, TAG_GIT_URI, property_set=self.property_set, default=""
        )
        self.git_branch = get_tag_el_text(
            self.oozie_node, TAG_BRANCH, property_set=self.property_set, default=""
        )
        self.destination_uri = get_tag_el_text(
            self.oozie_node, tag=TAG_DESTINATION_URI, property_set=self.property_set, default=""
        )
        self.destination_path = urlparse(self.destination_uri).path
        self.key_path_uri = get_tag_el_text(
            self.oozie_node, tag=TAG_KEY_PATH, property_set=self.property_set, default=""
        )
        self.key_path = urlparse(self.key_path_uri).path

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        action_task = Task(
            task_id=self.name,
            template_name="git.tpl",
            template_params=dict(
                git_uri=self.git_uri,
                git_branch=self.git_branch,
                destination_uri=self.destination_uri,
                destination_path=self.destination_path,
                key_path_uri=self.key_path_uri,
                key_path=self.key_path,
                property_set=self.property_set,
            ),
        )
        tasks: List[Task] = [action_task]
        relations: List[Relation] = []
        prepare_task = self.get_prepare_task(
            name=self.name, trigger_rule=self.trigger_rule, property_set=self.property_set
        )
        if prepare_task:
            relations = [Relation(prepare_task.task_id, to_task_id=self.name)]
            tasks = [prepare_task, action_task]
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self) -> str:
        return f"{self.name}_prepare" if self.has_prepare() else self.name
