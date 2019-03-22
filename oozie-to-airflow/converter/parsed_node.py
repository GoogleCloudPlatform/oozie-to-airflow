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

# noinspection PyPackageRequirements
from airflow.utils.trigger_rule import TriggerRule
import logging


class ParsedNode(object):
    def __init__(self, operator):
        self.operator = operator
        self.parsed_downstream = []
        self.downstream_names = []
        self.is_error = False
        self.is_ok = False
        self.error_xml = None

    def add_downstream_node_name(self, node_name):
        """
        Adds a single downstream name string to the list `downstream_names`.
        :param node_name: The name to append to the list
        """
        self.downstream_names.append(node_name)

    def set_error_node_name(self, error_name):
        """
        Sets the error_xml class variable to the supplied `error_name` 
        :param error_name: The downstream error node, Oozie nodes can only have
            one error downstream.
        """
        self.error_xml = error_name

    def get_downstreams(self):
        return self.downstream_names

    def get_error_downstream_name(self):
        return self.error_xml

    def set_is_error(self, is_error):
        """
        A bit that switches when the node is the error downstream of any other
        node.
        :param is_error: Boolean of is_error or not.
        """
        self.is_error = is_error

    def set_is_ok(self, is_ok):
        """
        A bit that switches when the node is the ok downstream of any other
        node.
        :param is_ok: Boolean of is_ok or not.
        """
        self.is_ok = is_ok

    def update_trigger_rule(self):
        """
        The trigger rule gets determined by if it is error or ok.

        OK only -> TriggerRule.ONE_SUCCESS
        ERROR only -> TriggerRule.ONE_FAILED
        both -> TriggerRule.DUMMY and a warning log.
        neither -> TriggerRule.DUMMY

        Eventually this if it is both error and ok, then
        we can extend it into two Airflow Operators where one
        is a python branch operator, and make a decision there.
        """
        if self.is_ok and self.is_error:
            logging.warning(
                "Task {} is both an error node and a ok node.".format(self.operator.get_task_id())
            )
            self.operator.trigger_rule = TriggerRule.DUMMY
        elif not self.is_ok and not self.is_error:
            # Sets to dummy, but does not warn user about it.
            self.operator.trigger_rule = TriggerRule.DUMMY
        elif self.is_ok:
            self.operator.trigger_rule = TriggerRule.ONE_SUCCESS
        else:
            self.operator.trigger_rule = TriggerRule.ONE_FAILED
