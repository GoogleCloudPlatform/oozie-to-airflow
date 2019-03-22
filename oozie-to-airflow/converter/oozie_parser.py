# Copyright 2018 Google LLC
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

from collections import OrderedDict
import logging
import xml.etree.ElementTree as ET

import uuid

# noinspection PyPackageRequirements
from airflow.utils.trigger_rule import TriggerRule
from converter import parsed_node
import mappers.decision_mapper
import mappers.dummy_mapper
import mappers.kill_mapper
import mappers.null_mapper
import mappers.spark_mapper
import mappers.ssh_mapper
import mappers.pig_mapper
import mappers.subwf_mapper
import utils.xml_utils
from converter.parsed_node import ParsedNode

CONTROL_MAP = {
    "decision": mappers.decision_mapper.DecisionMapper,
    "end": mappers.dummy_mapper.DummyMapper,
    "kill": mappers.kill_mapper.KillMapper,
    "fork": mappers.dummy_mapper.DummyMapper,
    "join": mappers.dummy_mapper.DummyMapper,
    "start": mappers.dummy_mapper.DummyMapper,
}

ACTION_MAP = {
    "unknown": mappers.dummy_mapper.DummyMapper,
    "ssh": mappers.ssh_mapper.SSHMapper,
    "spark": mappers.spark_mapper.SparkMapper,
    "pig": mappers.pig_mapper.PigMapper,
    "sub-workflow": mappers.subwf_mapper.SubworkflowMapper,
}


# noinspection PyDefaultArgument
class OozieParser(object):
    def __init__(self, oozie_wflow, params, action_mapper=ACTION_MAP, control_map=CONTROL_MAP, dag_name=None):
        self.workflow = oozie_wflow
        self.ACTION_MAP = action_mapper
        self.CONTROL_MAP = control_map
        self.PARAMS = params
        # Dictionary is ordered purely for output being somewhat ordered the
        # same as how Oozie workflow was parsed.
        self.OPERATORS = OrderedDict()
        # These are the general dependencies required that every operator
        # requires. The o2a_libs are for the external EL function parsing.
        self.DEPENDENCIES = {
            "import datetime",
            "from airflow import models",
            "from airflow.utils.trigger_rule import TriggerRule",
        }
        self.relations = set()
        self.dag_name = dag_name

    def _parse_kill_node(self, kill_node):
        """
        When a workflow node reaches the `kill` node, it finishes in an error.
        A workflow definition may have zero or more kill nodes.
        """
        map_class = self.CONTROL_MAP["kill"]
        operator = map_class(None, kill_node.attrib["name"], TriggerRule.ONE_FAILED)
        p_node = parsed_node.ParsedNode(operator)

        logging.info("Parsed {} as Kill Node.".format(operator.task_id))
        self.OPERATORS[kill_node.attrib["name"]] = p_node
        self.DEPENDENCIES.update(operator.required_imports())

    def _parse_end_node(self, end_node):
        """
        Upon reaching the end node, the workflow is considered completed successfully.
        Thus it gets mapped to a dummy node that always completes.
        """
        map_class = self.CONTROL_MAP["end"]
        operator = map_class(end_node, end_node.attrib["name"])
        p_node = parsed_node.ParsedNode(operator)

        logging.info("Parsed {} as End Node.".format(operator.task_id))
        self.OPERATORS[end_node.attrib["name"]] = p_node
        self.DEPENDENCIES.update(operator.required_imports())

    def _parse_fork_node(self, root, fork_node):
        """
        Fork nodes need to be dummy operators with multiple parallel downstream
        tasks.

        This parses the fork node, the action nodes that it references and then
        the join node at the end.

        This will only parse well-formed xml-adhering workflows where all paths
        end at the join node.
        """
        map_class = self.CONTROL_MAP["fork"]
        fork_name = fork_node.attrib["name"]
        fork_start_op = map_class(fork_node, fork_name)
        p_node = parsed_node.ParsedNode(fork_start_op)

        logging.info("Parsed {} as Fork Node.".format(fork_start_op.task_id))
        paths = []
        for node in fork_node:
            if "path" in node.tag:
                # Parse all the downstream tasks that can run in parallel.
                curr_name = node.attrib["start"]
                paths.append(utils.xml_utils.find_node_by_name(root, curr_name))

        self.OPERATORS[fork_name] = p_node
        self.DEPENDENCIES.update(fork_start_op.required_imports())

        for path in paths:
            p_node.add_downstream_node_name(path.attrib["name"])
            logging.info("Added {}'s downstream: {}".format(fork_start_op.task_id, path.attrib["name"]))
            # Theoretically these will all be action nodes, however I don't
            # think that is guaranteed.
            # The end of the execution path has not been reached
            self.parse_node(root, path)
            if path.attrib["name"] not in self.OPERATORS:
                root.remove(path)

    def _parse_join_node(self, join_node):
        """
        Join nodes wait for the corresponding beginning fork node paths to
        finish. As the parser we are assuming the Oozie workflow follows the
        schema perfectly.
        """
        map_class = self.CONTROL_MAP["join"]
        operator = map_class(join_node, join_node.attrib["name"])

        p_node = parsed_node.ParsedNode(operator)
        p_node.add_downstream_node_name(join_node.attrib["to"])

        logging.info("Parsed {} as Join Node.".format(operator.task_id))
        self.OPERATORS[join_node.attrib["name"]] = p_node
        self.DEPENDENCIES.update(operator.required_imports())

    def _parse_decision_node(self, decision_node):
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
        map_class = self.CONTROL_MAP["decision"]
        operator = map_class(decision_node, decision_node.attrib["name"])

        p_node = parsed_node.ParsedNode(operator)
        for cases in decision_node[0]:
            p_node.add_downstream_node_name(cases.attrib["to"])

        logging.info("Parsed {} as Decision Node.".format(operator.task_id))
        self.OPERATORS[decision_node.attrib["name"]] = p_node
        self.DEPENDENCIES.update(operator.required_imports())

    def _parse_action_node(self, action_node):
        """
        Action nodes are the mechanism by which a workflow triggers the
        execution of a computation/processing task.

        Action nodes are required to have an action-choice (map-reduce, etc.),
        ok, and error node in the xml.
        """
        # The 0th element of the node is the actual action tag.
        # In the form of 'action'
        action_name = action_node[0].tag

        if action_name not in self.ACTION_MAP:
            action_name = "unknown"

        map_class = self.ACTION_MAP[action_name]
        operator = map_class(
            action_node[0], action_node.attrib["name"], params=self.PARAMS, dag_name=self.dag_name
        )

        p_node = parsed_node.ParsedNode(operator)
        p_node.add_downstream_node_name(action_node[1].attrib["to"])  # 'ok' node
        p_node.set_error_node_name(action_node[2].attrib["to"])  # 'fail' node

        logging.info("Parsed {} as Action Node of type {}.".format(operator.task_id, action_name))
        self.DEPENDENCIES.update(operator.required_imports())

        # TODO A hacky way to get the correct control flow for now, fix
        if operator.has_prepare():
            print(operator.task_id)
            self.OPERATORS[operator.task_id] = ParsedNode(mappers.null_mapper.NullMapper(operator.task_id))

        self.OPERATORS[operator.get_task_id()] = p_node

    def _parse_start_node(self, start_node):
        """
        The start node is the entry point for a workflow job, it indicates the
        first workflow node the workflow job must transition to.

        When a workflow is started, it automatically transitions to the
        node specified in the start.

        A workflow definition must have one start node.
        """
        map_class = self.CONTROL_MAP["start"]
        # Theoretically this could cause conflicts, but it is very unlikely
        start_name = "start_node_" + str(uuid.uuid4())[:4]
        operator = map_class(start_node, task_id=start_name)

        p_node = parsed_node.ParsedNode(operator)
        p_node.add_downstream_node_name(start_node.attrib["to"])

        logging.info("Parsed {} as Start Node.".format(operator.task_id))
        self.OPERATORS[start_name] = p_node
        self.DEPENDENCIES.update(operator.required_imports())

    def parse_node(self, root, node):
        """
        Given a node, determines its tag, and then passes it to the correct
        parser.

        :param root:  The root node of the XML tree.
        :param node: The node to parse.
        """
        if "action" in node.tag:
            self._parse_action_node(node)
        elif "start" in node.tag:
            self._parse_start_node(node)
        elif "kill" in node.tag:
            self._parse_kill_node(node)
        elif "end" in node.tag:
            self._parse_end_node(node)
        elif "fork" in node.tag:
            self._parse_fork_node(root, node)
        elif "join" in node.tag:
            self._parse_join_node(node)
        elif "decision" in node.tag:
            self._parse_decision_node(node)

    def parse_workflow(self):
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

    def create_relations(self):
        """
        Given a dictionary of task_ids and ParsedNodes,
        returns a set of logical connectives for each task in Airflow.
        :return: Set with strings of task's downstream nodes.
        """
        ops = self.OPERATORS
        logging.info("Parsing relations between operators.")
        for node_name, p_node in ops.items():
            for downstream in p_node.get_downstreams():
                ok_str = "{}.set_downstream({})".format(node_name, downstream)
                self.relations.add(ok_str)
            if p_node.get_error_downstream_name():
                error_str = "{}.set_downstream({})".format(node_name, p_node.get_error_downstream_name())
                self.relations.add(error_str)

    def update_trigger_rules(self):
        """
        Updates the trigger rules of each node based on the downstream and
        error nodes.
        """
        for operator in self.OPERATORS.values():
            # If a task is referenced  by an "ok to=<task>", flip bit in parsed
            # node class
            for downstream in operator.get_downstreams():
                self.OPERATORS[downstream].set_is_ok(True)
            error_name = operator.get_error_downstream_name()
            if error_name:
                # If a task is referenced  by an "error to=<task>", flip
                # corresponding bit in the parsed node class
                self.OPERATORS[error_name].set_is_error(True)
            operator.update_trigger_rule()

    def get_relations(self):
        if len(self.relations) == 0:
            self.create_relations()

        return self.relations

    def get_dependencies(self):
        return self.DEPENDENCIES

    def get_operators(self):
        return self.OPERATORS
