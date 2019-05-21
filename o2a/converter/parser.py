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
"""Parsing module """
import os
import logging

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

import uuid

# noinspection PyPackageRequirements
from typing import Type, Dict

from airflow.utils.trigger_rule import TriggerRule
from o2a.utils import xml_utils
from o2a.converter.constants import HDFS_FOLDER
from o2a.converter.parsed_node import ParsedNode
from o2a.converter.workflow import Workflow
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.base_mapper import BaseMapper


# noinspection PyDefaultArgument
class OozieParser:
    """Parses XML of an Oozie workflow"""

    control_map: Dict[str, Type[BaseMapper]]
    action_map: Dict[str, Type[ActionMapper]]
    params: Dict[str, str]

    def __init__(
        self,
        input_directory_path: str,
        output_directory_path: str,
        params: Dict[str, str],
        action_mapper: Dict[str, Type[ActionMapper]],
        control_mapper: Dict[str, Type[BaseMapper]],
        dag_name: str = None,
    ):
        self.workflow = Workflow(
            dag_name=dag_name,
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
        )
        self.workflow_file = os.path.join(input_directory_path, HDFS_FOLDER, "workflow.xml")
        self.params = params
        self.action_map = action_mapper
        self.control_map = control_mapper

    def parse_kill_node(self, kill_node: ET.Element):
        """
        When a workflow node reaches the `kill` node, it finishes in an error.
        A workflow definition may have zero or more kill nodes.
        """
        map_class = self.control_map["kill"]
        mapper = map_class(
            oozie_node=kill_node, name=kill_node.attrib["name"], trigger_rule=TriggerRule.ONE_FAILED
        )
        p_node = ParsedNode(mapper)

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Kill Node.")
        self.workflow.nodes[kill_node.attrib["name"]] = p_node
        self.workflow.dependencies.update(mapper.required_imports())

    def parse_end_node(self, end_node):
        """
        Upon reaching the end node, the workflow is considered completed successfully.
        Thus it gets mapped to a dummy node that always completes.
        """
        map_class = self.control_map["end"]
        mapper = map_class(oozie_node=end_node, name=end_node.attrib["name"])
        p_node = ParsedNode(mapper)

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as End Node.")
        self.workflow.nodes[end_node.attrib["name"]] = p_node
        self.workflow.dependencies.update(mapper.required_imports())

    def parse_fork_node(self, root, fork_node):
        """
        Fork nodes need to be dummy operators with multiple parallel downstream
        tasks.

        This parses the fork node, the action nodes that it references and then
        the join node at the end.

        This will only parse well-formed xml-adhering workflows where all paths
        end at the join node.
        """
        map_class = self.control_map["fork"]
        fork_name = fork_node.attrib["name"]
        mapper = map_class(oozie_node=fork_node, name=fork_name)
        p_node = ParsedNode(mapper)

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Fork Node.")
        paths = []
        for node in fork_node:
            if "path" in node.tag:
                # Parse all the downstream tasks that can run in parallel.
                curr_name = node.attrib["start"]
                paths.append(xml_utils.find_node_by_name(root, curr_name))

        self.workflow.nodes[fork_name] = p_node
        self.workflow.dependencies.update(mapper.required_imports())

        for path in paths:
            p_node.add_downstream_node_name(path.attrib["name"])
            logging.info(f"Added {mapper.name}'s downstream: {path.attrib['name']}")

            # Theoretically these will all be action nodes, however I don't
            # think that is guaranteed.
            # The end of the execution path has not been reached
            self.parse_node(root, path)
            if path.attrib["name"] not in self.workflow.nodes:
                root.remove(path)

    def parse_join_node(self, join_node):
        """
        Join nodes wait for the corresponding beginning fork node paths to
        finish. As the parser we are assuming the Oozie workflow follows the
        schema perfectly.
        """
        map_class = self.control_map["join"]
        mapper = map_class(oozie_node=join_node, name=join_node.attrib["name"])

        p_node = ParsedNode(mapper)
        p_node.add_downstream_node_name(join_node.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Join Node.")
        self.workflow.nodes[join_node.attrib["name"]] = p_node
        self.workflow.dependencies.update(mapper.required_imports())

    def parse_decision_node(self, decision_node):
        """
        A decision node enables a workflow to make a selection on the execution
        path to follow.

        The behavior of a decision node can be seen as a switch-case statement.

        A decision node consists of a list of predicates-transition pairs plus
        a default transition. Predicates are evaluated in order or appearance
        until one of them evaluates to true and the corresponding transition is
        taken. If none of the predicates evaluates to true the default
        transition is taken.

        example oozie wf decision node:

        <decision name="[NODE-NAME]">
            <switch>
                <case to="[NODE_NAME]">[PREDICATE]</case>
                ...
                <case to="[NODE_NAME]">[PREDICATE]</case>
                <default to="[NODE_NAME]"/>
            </switch>
        </decision>
        """
        map_class = self.control_map["decision"]
        mapper = map_class(oozie_node=decision_node, name=decision_node.attrib["name"])

        p_node = ParsedNode(mapper)
        for cases in decision_node[0]:
            p_node.add_downstream_node_name(cases.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Decision Node.")
        self.workflow.nodes[decision_node.attrib["name"]] = p_node
        self.workflow.dependencies.update(mapper.required_imports())

    def parse_action_node(self, action_node: ET.Element):
        """
        Action nodes are the mechanism by which a workflow triggers the
        execution of a computation/processing task.

        Action nodes are required to have an action-choice (map-reduce, etc.),
        ok, and error node in the xml.
        """
        # The 0th element of the node is the actual action tag.
        # In the form of 'action'
        action_operation_node = action_node[0]
        action_name = action_operation_node.tag

        if action_name not in self.action_map:
            action_name = "unknown"

        map_class = self.action_map[action_name]
        mapper = map_class(
            oozie_node=action_operation_node,
            name=action_node.attrib["name"],
            params=self.params,
            dag_name=self.workflow.dag_name,
            action_mapper=self.action_map,
            control_mapper=self.control_map,
            input_directory_path=self.workflow.input_directory_path,
            output_directory_path=self.workflow.output_directory_path,
        )

        p_node = ParsedNode(mapper)
        ok_node = action_node.find("ok")
        if ok_node is None:
            raise Exception("Missing ok node in {}".format(action_node))
        p_node.add_downstream_node_name(ok_node.attrib["to"])
        error_node = action_node.find("error")
        if error_node is None:
            raise Exception("Missing error node in {}".format(action_node))
        p_node.set_error_node_name(error_node.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Action Node of type {action_name}.")
        self.workflow.dependencies.update(mapper.required_imports())

        self.workflow.nodes[mapper.name] = p_node

    def parse_start_node(self, start_node):
        """
        The start node is the entry point for a workflow job, it indicates the
        first workflow node the workflow job must transition to.

        When a workflow is started, it automatically transitions to the
        node specified in the start.

        A workflow definition must have one start node.
        """
        map_class = self.control_map["start"]
        # Theoretically this could cause conflicts, but it is very unlikely
        start_name = "start_node_" + str(uuid.uuid4())[:4]
        mapper = map_class(oozie_node=start_node, name=start_name)

        p_node = ParsedNode(mapper)
        p_node.add_downstream_node_name(start_node.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Start Node.")
        self.workflow.nodes[start_name] = p_node
        self.workflow.dependencies.update(mapper.required_imports())

    def parse_node(self, root, node):
        """
        Given a node, determines its tag, and then passes it to the correct
        parser.

        :param root:  The root node of the XML tree.
        :param node: The node to parse.
        """
        if "action" in node.tag:
            self.parse_action_node(node)
        elif "start" in node.tag:
            self.parse_start_node(node)
        elif "kill" in node.tag:
            self.parse_kill_node(node)
        elif "end" in node.tag:
            self.parse_end_node(node)
        elif "fork" in node.tag:
            self.parse_fork_node(root, node)
        elif "join" in node.tag:
            self.parse_join_node(node)
        elif "decision" in node.tag:
            self.parse_decision_node(node)

    def parse_workflow(self):
        """Parses workflow replacing invalid characters in the names of the nodes"""
        tree = ET.parse(self.workflow_file)
        root = tree.getroot()
        for node in tree.iter():
            # Strip namespaces
            node.tag = node.tag.split("}")[1][0:]

            # Change names to python syntax
            if "name" in node.attrib:
                node.attrib["name"] = node.attrib["name"].replace("-", "_")
            if "to" in node.attrib:
                node.attrib["to"] = node.attrib["to"].replace("-", "_")
            if "error" in node.attrib:
                node.attrib["error"] = node.attrib["error"].replace("-", "_")
            if "start" in node.attrib:
                node.attrib["start"] = node.attrib["start"].replace("-", "_")

        logging.info("Stripped namespaces, and replaced invalid characters.")

        for node in root:
            logging.debug(f"Parsing node: {node}")
            self.parse_node(root, node)

        self.create_relations()
        self.update_trigger_rules()

        for node in self.workflow.nodes.copy().values():
            node.mapper.on_parse_finish(self.workflow)

    def create_relations(self) -> None:
        """
        Given a dictionary of task_ids and ParsedNodes,
        returns a set of logical connectives for each task in Airflow.

        :return: Set with strings of task's downstream nodes.
        """
        logging.info("Parsing relations between operators.")
        for p_node in self.workflow.nodes.values():
            for downstream in p_node.get_downstreams():
                relation = Relation(
                    from_task_id=p_node.last_task_id, to_task_id=self.workflow.nodes[downstream].first_task_id
                )
                self.workflow.relations.add(relation)
            error_downstream = p_node.get_error_downstream_name()
            if error_downstream:
                relation = Relation(
                    from_task_id=p_node.last_task_id,
                    to_task_id=self.workflow.nodes[error_downstream].first_task_id,
                )
                self.workflow.relations.add(relation)

    def update_trigger_rules(self) -> None:
        """
        Updates the trigger rules of each node based on the downstream and
        error nodes.
        """
        for node in self.workflow.nodes.values():
            # If a task is referenced  by an "ok to=<task>", flip bit in parsed
            # node class
            for downstream in node.get_downstreams():
                self.workflow.nodes[downstream].set_is_ok(True)
            error_name = node.get_error_downstream_name()
            if error_name:
                # If a task is referenced  by an "error to=<task>", flip
                # corresponding bit in the parsed node class
                self.workflow.nodes[error_name].set_is_error(True)
            node.update_trigger_rule()
