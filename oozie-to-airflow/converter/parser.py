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
from collections import OrderedDict
import logging

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

import uuid

# noinspection PyPackageRequirements
from typing import Type, Dict, Set

from airflow.utils.trigger_rule import TriggerRule
import utils.xml_utils
from converter.parsed_node import ParsedNode
from mappers.action_mapper import ActionMapper
from mappers.base_mapper import BaseMapper
from mappers.file_archive_mixins import FileMixin, ArchiveMixin
from mappers.null_mapper import NullMapper


# noinspection PyDefaultArgument
class OozieParser:
    """Parses XML of an Oozie workflow"""

    control_map: Dict[str, Type[BaseMapper]]
    action_map: Dict[str, Type[ActionMapper]]
    relations: Set[str]
    params: Dict[str, str]
    dependencies: Set[str]  # TODO: Check is set likely maintain insertion order (Python 3.6 ?)
    operators: Dict[str, ParsedNode]

    def __init__(
        self,
        input_directory_path: str,
        output_directory_path: str,
        params: Dict[str, str],
        action_mapper: Dict[str, Type[ActionMapper]],
        control_mapper: Dict[str, Type[BaseMapper]],
        dag_name: str = None,
    ):
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.workflow = os.path.join(input_directory_path, "workflow.xml")
        self.action_map = action_mapper
        self.control_map = control_mapper
        self.params = params
        # Dictionary is ordered purely for output being somewhat ordered the
        # same as how Oozie workflow was parsed.
        self.operators = OrderedDict()
        # These are the general dependencies required that every operator
        # requires. The o2a_libs are for the external EL function parsing.
        self.dependencies = {
            "import datetime",
            "from airflow import models",
            "from airflow.utils.trigger_rule import TriggerRule",
        }
        self.relations = set()
        self.dag_name = dag_name

    def parse_kill_node(self, kill_node: ET.Element):
        """
        When a workflow node reaches the `kill` node, it finishes in an error.
        A workflow definition may have zero or more kill nodes.
        """
        map_class = self.control_map["kill"]
        operator = map_class(
            oozie_node=kill_node, task_id=kill_node.attrib["name"], trigger_rule=TriggerRule.ONE_FAILED
        )
        p_node = ParsedNode(operator)

        logging.info("Parsed {} as Kill Node.".format(operator.task_id))
        self.operators[kill_node.attrib["name"]] = p_node
        self.dependencies.update(operator.required_imports())

    def parse_end_node(self, end_node):
        """
        Upon reaching the end node, the workflow is considered completed successfully.
        Thus it gets mapped to a dummy node that always completes.
        """
        map_class = self.control_map["end"]
        operator = map_class(oozie_node=end_node, task_id=end_node.attrib["name"])
        p_node = ParsedNode(operator)

        logging.info("Parsed {} as End Node.".format(operator.task_id))
        self.operators[end_node.attrib["name"]] = p_node
        self.dependencies.update(operator.required_imports())

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
        fork_start_op = map_class(oozie_node=fork_node, task_id=fork_name)
        p_node = ParsedNode(fork_start_op)

        logging.info("Parsed {} as Fork Node.".format(fork_start_op.task_id))
        paths = []
        for node in fork_node:
            if "path" in node.tag:
                # Parse all the downstream tasks that can run in parallel.
                curr_name = node.attrib["start"]
                paths.append(utils.xml_utils.find_node_by_name(root, curr_name))

        self.operators[fork_name] = p_node
        self.dependencies.update(fork_start_op.required_imports())

        for path in paths:
            p_node.add_downstream_node_name(path.attrib["name"])
            logging.info("Added {}'s downstream: {}".format(fork_start_op.task_id, path.attrib["name"]))
            # Theoretically these will all be action nodes, however I don't
            # think that is guaranteed.
            # The end of the execution path has not been reached
            self.parse_node(root, path)
            if path.attrib["name"] not in self.operators:
                root.remove(path)

    def parse_join_node(self, join_node):
        """
        Join nodes wait for the corresponding beginning fork node paths to
        finish. As the parser we are assuming the Oozie workflow follows the
        schema perfectly.
        """
        map_class = self.control_map["join"]
        operator = map_class(oozie_node=join_node, task_id=join_node.attrib["name"])

        p_node = ParsedNode(operator)
        p_node.add_downstream_node_name(join_node.attrib["to"])

        logging.info("Parsed {} as Join Node.".format(operator.task_id))
        self.operators[join_node.attrib["name"]] = p_node
        self.dependencies.update(operator.required_imports())

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
        operator = map_class(oozie_node=decision_node, task_id=decision_node.attrib["name"])

        p_node = ParsedNode(operator)
        for cases in decision_node[0]:
            p_node.add_downstream_node_name(cases.attrib["to"])

        logging.info("Parsed {} as Decision Node.".format(operator.task_id))
        self.operators[decision_node.attrib["name"]] = p_node
        self.dependencies.update(operator.required_imports())

    def parse_action_node(self, action_node: ET.Element):
        """
        Action nodes are the mechanism by which a workflow triggers the
        execution of a computation/processing task.

        Action nodes are required to have an action-choice (map-reduce, etc.),
        ok, and error node in the xml.
        """
        # The 0th element of the node is the actual action tag.
        # In the form of 'action'
        action_name = action_node[0].tag

        if action_name not in self.action_map:
            action_name = "unknown"

        map_class = self.action_map[action_name]
        operator = map_class(
            oozie_node=action_node[0],
            task_id=action_node.attrib["name"],
            params=self.params,
            dag_name=self.dag_name,
            input_directory_path=self.input_directory_path,
            output_directory_path=self.output_directory_path,
        )

        p_node = ParsedNode(operator)
        ok_node = action_node.find("ok")
        if ok_node is None:
            raise Exception("Missing ok node in {}".format(action_node))
        p_node.add_downstream_node_name(ok_node.attrib["to"])
        error_node = action_node.find("error")
        if error_node is None:
            raise Exception("Missing error node in {}".format(action_node))
        p_node.set_error_node_name(error_node.attrib["to"])

        self._parse_file_nodes(action_node, operator)

        self._parse_archive_nodes(action_node, operator)

        logging.info("Parsed {} as Action Node of type {}.".format(operator.task_id, action_name))
        self.dependencies.update(operator.required_imports())

        # TODO A hacky way to get the correct control flow for now, fix
        if operator.has_prepare:
            print(operator.task_id)
            self.operators[operator.task_id] = ParsedNode(
                NullMapper(oozie_node=action_node, task_id=operator.task_id)
            )

        self.operators[operator.get_task_id()] = p_node

    @staticmethod
    def _parse_file_nodes(action_node, operator: ActionMapper):
        file_nodes = action_node.findall("file")
        if file_nodes:
            if isinstance(operator, FileMixin):
                for file_node in file_nodes:
                    file_path = file_node.text
                    operator.add_file(file_path)
            else:
                raise Exception("The operator {} does not derive from FileMixin".format(operator))

    @staticmethod
    def _parse_archive_nodes(action_node, operator: ActionMapper):
        archive_nodes = action_node.findall("archive")
        if archive_nodes:
            if isinstance(operator, ArchiveMixin):
                for archive_node in archive_nodes:
                    archive_path = archive_node.text
                    operator.add_archive(archive_path)
            else:
                raise Exception("The operator {} does not derive from ArchiveMixin".format(operator))

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
        operator = map_class(oozie_node=start_node, task_id=start_name)

        p_node = ParsedNode(operator)
        p_node.add_downstream_node_name(start_node.attrib["to"])

        logging.info("Parsed {} as Start Node.".format(operator.task_id))
        self.operators[start_name] = p_node
        self.dependencies.update(operator.required_imports())

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
        tree = ET.parse(self.workflow)
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
            logging.debug("Parsing node: {}".format(node))
            self.parse_node(root, node)

    def create_relations(self) -> None:
        """
        Given a dictionary of task_ids and ParsedNodes,
        returns a set of logical connectives for each task in Airflow.
        :return: Set with strings of task's downstream nodes.
        """
        ops = self.operators
        logging.info("Parsing relations between operators.")
        for node_name, p_node in ops.items():
            for downstream in p_node.get_downstreams():
                ok_str = "{}.set_downstream({})".format(node_name, downstream)
                self.relations.add(ok_str)
            if p_node.get_error_downstream_name():
                error_str = "{}.set_downstream({})".format(node_name, p_node.get_error_downstream_name())
                self.relations.add(error_str)

    def update_trigger_rules(self) -> None:
        """
        Updates the trigger rules of each node based on the downstream and
        error nodes.
        """
        for operator in self.operators.values():
            # If a task is referenced  by an "ok to=<task>", flip bit in parsed
            # node class
            for downstream in operator.get_downstreams():
                self.operators[downstream].set_is_ok(True)
            error_name = operator.get_error_downstream_name()
            if error_name:
                # If a task is referenced  by an "error to=<task>", flip
                # corresponding bit in the parsed node class
                self.operators[error_name].set_is_error(True)
            operator.update_trigger_rule()

    def get_relations(self) -> Set[str]:
        if not self.relations:
            self.create_relations()
        return self.relations

    def get_dependencies(self) -> Set[str]:
        return self.dependencies

    def get_operators(self) -> Dict[str, ParsedNode]:
        return self.operators
