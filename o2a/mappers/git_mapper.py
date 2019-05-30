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

from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
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
        "--cluster {{params.config['dataproc_cluster']}} "
        "--region {{params.config['gcp_region']}} "
        f"--git-uri {shlex.quote(git_uri)} "
        f"--destination-path {shlex.quote(destination_path)}"
    )

    if git_branch:
        cmd += f" --branch {shlex.quote(git_branch)}"

    if key_path:
        cmd += f" --key-path {shlex.quote(key_path)}"

    return cmd


class GitMapper(ActionMapper):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        props: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, props=props, **kwargs
        )
        self.git_uri: Optional[str] = None
        self.git_branch: Optional[str] = None
        self.destination_uri: Optional[str] = None
        self.destination_path: Optional[str] = None
        self.key_path_uri: Optional[str] = None
        self.key_path: Optional[str] = None
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def on_parse_node(self):
        super().on_parse_node()
        self.git_uri = get_tag_el_text(self.oozie_node, TAG_GIT_URI, props=self.props, default="")
        self.git_branch = get_tag_el_text(self.oozie_node, TAG_BRANCH, props=self.props, default="")
        self.destination_uri = get_tag_el_text(
            self.oozie_node, tag=TAG_DESTINATION_URI, props=self.props, default=""
        )
        self.destination_path = urlparse(self.destination_uri).path
        self.key_path_uri = get_tag_el_text(self.oozie_node, tag=TAG_KEY_PATH, props=self.props, default="")
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
                props=self.props,
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self) -> str:
        return self.prepare_extension.first_task_id  # type: ignore  # make mypy happy
