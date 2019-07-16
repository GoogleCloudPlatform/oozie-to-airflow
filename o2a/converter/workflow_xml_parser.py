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
import logging
import os
import uuid

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

# noinspection PyPackageRequirements
from typing import Dict, List, Type

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.constants import HDFS_FOLDER
from o2a.converter.oozie_node import OozieActionNode, OozieControlNode
from o2a.converter.renderers import BaseRenderer
from o2a.converter.workflow import Workflow
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.decision_mapper import DecisionMapper
from o2a.mappers.dummy_mapper import DummyMapper
from o2a.mappers.end_mapper import EndMapper
from o2a.mappers.fork_mapper import ForkMapper
from o2a.mappers.join_mapper import JoinMapper
from o2a.mappers.kill_mapper import KillMapper
from o2a.mappers.start_mapper import StartMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.base_transformer import BaseWorkflowTransformer
from o2a.utils import xml_utils


# noinspection PyDefaultArgument
class WorkflowXmlParser:
    """Parses XML of an Oozie workflow"""

    def __init__(
        self,
        props: PropertySet,
        action_mapper: Dict[str, Type[ActionMapper]],
        renderer: BaseRenderer,
        workflow: Workflow,
        transformers: List[BaseWorkflowTransformer] = None,
    ):
        self.workflow = workflow
        self.workflow_file = os.path.join(workflow.input_directory_path, HDFS_FOLDER, "workflow.xml")
        self.props = props
        self.action_map = action_mapper
        self.renderer = renderer
        self.transformers = transformers

    def parse_kill_node(self, kill_node: ET.Element):
        """
        When a workflow node reaches the `kill` node, it finishes in an error.
        A workflow definition may have zero or more kill nodes.
        """
        mapper = KillMapper(
            oozie_node=kill_node,
            name=kill_node.attrib["name"],
            dag_name=self.workflow.dag_name,
            trigger_rule=TriggerRule.ONE_FAILED,
            props=self.props,
        )
        oozie_control_node = OozieControlNode(mapper)

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Kill Node.")
        self.workflow.nodes[kill_node.attrib["name"]] = oozie_control_node

    def parse_end_node(self, end_node):
        """
        Upon reaching the end node, the workflow is considered completed successfully.
        Thus it gets mapped to a dummy node that always completes.
        """
        mapper = EndMapper(oozie_node=end_node, name=end_node.attrib["name"], dag_name=self.workflow.dag_name)
        oozie_control_node = OozieControlNode(mapper)

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as End Node.")
        self.workflow.nodes[end_node.attrib["name"]] = oozie_control_node

    def parse_fork_node(self, root, fork_node):
        """
        Fork nodes need to be dummy operators with multiple parallel downstream
        tasks.

        This parses the fork node, the action nodes that it references and then
        the join node at the end.

        This will only parse well-formed xml-adhering workflows where all paths
        end at the join node.
        """
        fork_name = fork_node.attrib["name"]
        mapper = ForkMapper(oozie_node=fork_node, name=fork_name, dag_name=self.workflow.dag_name)
        oozie_control_node = OozieControlNode(mapper)

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Fork Node.")
        paths = []
        for node in fork_node:
            if "path" in node.tag:
                # Parse all the downstream tasks that can run in parallel.
                curr_name = node.attrib["start"]
                paths.append(xml_utils.find_node_by_name(root, curr_name))

        self.workflow.nodes[fork_name] = oozie_control_node

        for path in paths:
            oozie_control_node.downstream_names.append(path.attrib["name"])
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
        mapper = JoinMapper(
            oozie_node=join_node, name=join_node.attrib["name"], dag_name=self.workflow.dag_name
        )

        oozie_control_node = OozieControlNode(mapper)
        oozie_control_node.downstream_names.append(join_node.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Join Node.")
        self.workflow.nodes[join_node.attrib["name"]] = oozie_control_node

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
        mapper = DecisionMapper(
            oozie_node=decision_node,
            name=decision_node.attrib["name"],
            dag_name=self.workflow.dag_name,
            props=self.props,
        )

        oozie_control_node = OozieControlNode(mapper)
        for cases in decision_node[0]:
            oozie_control_node.downstream_names.append(cases.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Decision Node.")
        self.workflow.nodes[decision_node.attrib["name"]] = oozie_control_node

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

        mapper: BaseMapper
        if action_name not in self.action_map:
            action_name = "unknown"
            mapper = DummyMapper(
                oozie_node=action_operation_node,
                name=action_node.attrib["name"],
                dag_name=self.workflow.dag_name,
                props=self.props,
            )
        else:
            map_class = self.action_map[action_name]
            mapper = map_class(
                oozie_node=action_operation_node,
                name=action_node.attrib["name"],
                props=self.props,
                dag_name=self.workflow.dag_name,
                action_mapper=self.action_map,
                renderer=self.renderer,
                input_directory_path=self.workflow.input_directory_path,
                output_directory_path=self.workflow.output_directory_path,
                jar_files=self.workflow.jar_files,
                transformers=self.transformers,
            )

        oozie_action_node = OozieActionNode(mapper)
        ok_node = action_node.find("ok")
        if ok_node is None:
            raise Exception(f"Missing ok node in {action_node}")
        oozie_action_node.downstream_names.append(ok_node.attrib["to"])
        error_node = action_node.find("error")
        if error_node is None:
            raise Exception(f"Missing error node in {action_node}")
        oozie_action_node.error_downstream_name = error_node.attrib["to"]

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Action Node of type {action_name}.")

        self.workflow.nodes[mapper.name] = oozie_action_node

    def parse_start_node(self, start_node):
        """
        The start node is the entry point for a workflow job, it indicates the
        first workflow node the workflow job must transition to.

        When a workflow is started, it automatically transitions to the
        node specified in the start.

        A workflow definition must have one start node.
        """
        # Theoretically this could cause conflicts, but it is very unlikely
        start_name = "start_node_" + str(uuid.uuid4())[:4]
        mapper = StartMapper(
            oozie_node=start_node,
            name=start_name,
            dag_name=self.workflow.dag_name,
            props=self.props,
            trigger_rule=TriggerRule.DUMMY,
        )

        oozie_control_node = OozieControlNode(mapper)
        oozie_control_node.downstream_names.append(start_node.attrib["to"])

        mapper.on_parse_node()

        logging.info(f"Parsed {mapper.name} as Start Node.")
        self.workflow.nodes[start_name] = oozie_control_node

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

        logging.info("Stripped namespaces, and replaced invalid characters.")

        for node in root:
            logging.debug(f"Parsing node: {node}")
            self.parse_node(root, node)
